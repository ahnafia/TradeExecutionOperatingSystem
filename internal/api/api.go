// Package api is the HTTP surface: the only way anything other than the CLI reaches the
// engine.
//
// Two rules shape it.
//
// Handlers never ask who the caller is — the interceptor chain resolves identity and puts
// it in the context, so replacing a trusted header with real sessions later touches one
// function instead of every route (seam contract #2).
//
// Accept is acknowledged, not awaited. POST /api/orders returns as soon as the order is
// durable, which is the honest thing to report: matching happens behind a log and the
// result arrives on the fill stream. An endpoint that blocked until the order settled
// would turn a 4 ms acknowledgement into a 40 ms one and couple order entry to the
// availability of the matching engine.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ahnafia/trading-system/internal/book"
	"github.com/ahnafia/trading-system/internal/engine"
	"github.com/ahnafia/trading-system/internal/identity"
	"github.com/ahnafia/trading-system/internal/money"
	"github.com/ahnafia/trading-system/internal/pipeline"
)

// Config tunes the surface.
type Config struct {
	RequestsPerMinute int // per account; 0 disables
	MaxOpenOrders     int // per account; 0 disables
	SignupDeposit     money.Minor
	Symbols           []string
}

// DefaultConfig is the playground.
func DefaultConfig() Config {
	return Config{
		RequestsPerMinute: 240,
		MaxOpenOrders:     50,
		SignupDeposit:     money.Minor(100_000_00),
		Symbols:           []string{"ACME", "BETA", "CRUX"},
	}
}

// Server holds the dependencies every handler shares.
type Server struct {
	eng     *engine.Engine
	pl      *pipeline.Pipeline
	hub     *hub
	limiter *limiter
	cfg     Config
	auth    AuthConfig
	ident   *identity.Store
	http    *http.Client

	// trustHeader enables the Part 1 bypass. Off unless asked for by name.
	trustHeader bool
}

// New builds the API. Call Start to begin tailing the outcome log.
func New(pl *pipeline.Pipeline, cfg Config) *Server {
	return &Server{
		eng: pl.Engine, pl: pl, hub: newHub(pl.Log),
		limiter: newLimiter(), cfg: cfg,
		auth:  AuthConfigFromEnv(),
		ident: identity.New(pl.Engine.Pool()),
		// A bounded client: an OAuth provider that hangs must not hold a request open
		// indefinitely and consume a connection slot.
		http:        &http.Client{Timeout: 10 * time.Second},
		trustHeader: os.Getenv("TRADING_TRUST_HEADER") == "1",
	}
}

// AuthSummary describes the login posture, for the startup banner.
func (s *Server) AuthSummary() string {
	var modes []string
	if s.auth.gitHubEnabled() {
		modes = append(modes, "github oauth")
	}
	if s.auth.DevLogin {
		modes = append(modes, "dev login (NOT authentication)")
	}
	if s.trustHeader {
		modes = append(modes, "X-Account-Id header (IMPERSONATION ENABLED)")
	}
	if len(modes) == 0 {
		return "none configured — nobody can sign in"
	}
	return strings.Join(modes, " · ")
}

// Start begins the fill stream's tailers.
func (s *Server) Start(ctx context.Context) { s.hub.run(ctx) }

// Handler returns the routed, wrapped API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	s.authRoutes(mux)
	mux.HandleFunc("GET /api/me", s.handleMe)
	mux.HandleFunc("GET /api/symbols", s.handleSymbols)
	mux.HandleFunc("GET /api/book/{symbol}", s.handleBook)

	mux.HandleFunc("POST /api/orders", s.handleSubmit)
	mux.HandleFunc("GET /api/orders", s.handleOrders)
	mux.HandleFunc("GET /api/orders/{id}", s.handleOrder)
	mux.HandleFunc("POST /api/orders/{id}/cancel", s.handleCancel)

	mux.HandleFunc("GET /api/positions", s.handlePositions)
	mux.HandleFunc("GET /api/fills", s.handleFills)

	return chain(mux, s)
}

// --- accounts ---------------------------------------------------------------

// Accounts are provisioned by logging in, not by a separate signup call. One fewer way to
// come into existence means one fewer way to come into existence unauthenticated.

type meResp struct {
	AccountID   string `json:"account_id"`
	Cash        string `json:"cash"`
	Reserved    string `json:"reserved"`
	BuyingPower string `json:"buying_power"`
	FeesPaid    string `json:"fees_paid"`
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	account, ok := s.require(w, r)
	if !ok {
		return
	}
	b, err := s.eng.Balances(r.Context(), account)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, meResp{
		AccountID: account.String(), Cash: b.Cash.String(), Reserved: b.ReservedCash.String(),
		BuyingPower: b.BuyingPower.String(), FeesPaid: b.Fees.String(),
	})
}

// --- market data ------------------------------------------------------------

func (s *Server) handleSymbols(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"symbols": s.cfg.Symbols})
}

type level struct {
	Price string `json:"price"`
	Qty   string `json:"qty"`
}

func (s *Server) handleBook(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.PathValue("symbol"))
	b := s.pl.Matching.Book(symbol)
	if b == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"symbol": symbol, "bids": []level{}, "asks": []level{},
		})
		return
	}
	bids, asks := b.Depth(10)
	out := func(src []book.DepthLevel) []level {
		res := make([]level, 0, len(src))
		for _, l := range src {
			res = append(res, level{Price: l.Price.String(), Qty: l.Qty.String()})
		}
		return res
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"symbol": symbol, "bids": out(bids), "asks": out(asks),
	})
}

// --- orders -----------------------------------------------------------------

type submitReq struct {
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"` // buy | sell
	Type          string `json:"type"` // market | limit
	TIF           string `json:"tif"`  // ioc | gtc
	Qty           string `json:"qty"`  // "10", "0.5"
	LimitPrice    string `json:"limit_price,omitempty"`
	Venue         string `json:"venue,omitempty"`
}

type orderResp struct {
	OrderID      string `json:"order_id"`
	Status       string `json:"status"`
	Symbol       string `json:"symbol"`
	Side         string `json:"side"`
	Qty          string `json:"qty"`
	FilledQty    string `json:"filled_qty"`
	AvgPrice     string `json:"avg_price,omitempty"`
	RejectReason string `json:"reject_reason,omitempty"`
	Note         string `json:"note,omitempty"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	account, ok := s.require(w, r)
	if !ok {
		return
	}
	var req submitReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "expected a JSON body")
		return
	}

	// Abuse control that is about the ENGINE rather than the transport: an account with
	// thousands of resting orders is a memory cost in every book it touches.
	if s.cfg.MaxOpenOrders > 0 {
		open, err := s.eng.OpenOrderCount(r.Context(), account)
		if err == nil && open >= s.cfg.MaxOpenOrders {
			writeErr(w, http.StatusTooManyRequests, "TOO_MANY_OPEN_ORDERS",
				"cancel something before placing more")
			return
		}
	}

	er, err := s.buildSubmit(account, req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "INVALID_ORDER", err.Error())
		return
	}

	view, err := s.eng.Submit(r.Context(), er)
	if err != nil {
		var rej *engine.Rejection
		if errors.As(err, &rej) {
			// A rejection is an answer, not a failure. 422 says the request was
			// well-formed and the system declined it, which is exactly what happened.
			writeJSON(w, http.StatusUnprocessableEntity, orderResp{
				Status: "REJECTED", RejectReason: rej.Reason,
				Symbol: er.Symbol, Side: req.Side, Note: rej.Detail,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, orderResp{
		OrderID: view.ID.String(), Status: view.Status, Symbol: view.Symbol,
		Side: view.Side.String(), Qty: view.Qty.String(),
		FilledQty: view.FilledQty.String(), RejectReason: view.RejectReason,
		Note: "Accepted and durable. Fills arrive on GET /api/fills.",
	})
}

func (s *Server) buildSubmit(account uuid.UUID, req submitReq) (engine.SubmitRequest, error) {
	out := engine.SubmitRequest{AccountID: account, Venue: strings.ToUpper(req.Venue)}

	out.Symbol = strings.ToUpper(strings.TrimSpace(req.Symbol))
	if out.Symbol == "" {
		return out, errors.New("symbol is required")
	}
	out.ClientOrderID = strings.TrimSpace(req.ClientOrderID)
	if out.ClientOrderID == "" {
		// A client that supplies its own id gets retry-safety; one that does not still
		// gets an order, it just cannot safely retry it.
		out.ClientOrderID = uuid.NewString()
	}

	switch strings.ToLower(req.Side) {
	case "buy":
		out.Side = book.Buy
	case "sell":
		out.Side = book.Sell
	default:
		return out, errors.New(`side must be "buy" or "sell"`)
	}

	switch strings.ToLower(req.Type) {
	case "market":
		out.Type, out.TIF = book.Market, book.IOC
	case "limit":
		out.Type, out.TIF = book.Limit, book.GTC
	default:
		return out, errors.New(`type must be "market" or "limit"`)
	}
	switch strings.ToLower(req.TIF) {
	case "ioc":
		out.TIF = book.IOC
	case "gtc":
		out.TIF = book.GTC
	case "":
	default:
		return out, errors.New(`tif must be "ioc" or "gtc"`)
	}

	qty, err := money.ParseQty(req.Qty)
	if err != nil {
		return out, err
	}
	if qty <= 0 {
		return out, errors.New("qty must be positive")
	}
	out.Qty = qty

	if out.Type == book.Limit {
		if req.LimitPrice == "" {
			return out, errors.New("limit orders need a limit_price")
		}
		px, err := money.ParseMinor(req.LimitPrice)
		if err != nil {
			return out, err
		}
		if px <= 0 {
			return out, errors.New("limit_price must be positive")
		}
		out.LimitPrice = px
	}
	return out, nil
}

func (s *Server) handleOrder(w http.ResponseWriter, r *http.Request) {
	account, ok := s.require(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_ORDER_ID", "not a uuid")
		return
	}
	v, err := s.eng.Order(r.Context(), id)
	// Not-found and not-yours return the same thing on purpose: otherwise the endpoint
	// tells a stranger which order ids exist.
	if err != nil || v.AccountID != account {
		writeErr(w, http.StatusNotFound, "NO_SUCH_ORDER", "no such order")
		return
	}
	writeJSON(w, http.StatusOK, orderResp{
		OrderID: v.ID.String(), Status: v.Status, Symbol: v.Symbol, Side: v.Side.String(),
		Qty: v.Qty.String(), FilledQty: v.FilledQty.String(),
		AvgPrice: v.AvgPrice().String(), RejectReason: v.RejectReason,
	})
}

func (s *Server) handleOrders(w http.ResponseWriter, r *http.Request) {
	account, ok := s.require(w, r)
	if !ok {
		return
	}
	list, err := s.eng.RecentOrders(r.Context(), account, 100)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	out := make([]orderResp, 0, len(list))
	for _, v := range list {
		out = append(out, orderResp{
			OrderID: v.ID.String(), Status: v.Status, Symbol: v.Symbol,
			Side: v.Side.String(), Qty: v.Qty.String(),
			FilledQty: v.FilledQty.String(), AvgPrice: v.AvgPrice().String(),
			RejectReason: v.RejectReason,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": out})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	account, ok := s.require(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_ORDER_ID", "not a uuid")
		return
	}
	v, err := s.eng.Order(r.Context(), id)
	if err != nil || v.AccountID != account {
		writeErr(w, http.StatusNotFound, "NO_SUCH_ORDER", "no such order")
		return
	}

	status, err := s.eng.Cancel(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	// PENDING_CANCEL is the honest answer: the book has not ruled yet, and an in-flight
	// fill can still win. The verdict arrives on the fill stream.
	writeJSON(w, http.StatusAccepted, map[string]string{
		"order_id": id.String(), "status": status,
		"note": "The book decides; watch GET /api/fills for the verdict.",
	})
}

// --- positions --------------------------------------------------------------

type positionResp struct {
	Symbol     string `json:"symbol"`
	Qty        string `json:"qty"`
	Reserved   string `json:"reserved"`
	CostBasis  string `json:"cost_basis"`
	Mark       string `json:"mark,omitempty"`
	Realized   string `json:"realized_pnl"`
	Unrealized string `json:"unrealized_pnl,omitempty"`
}

func (s *Server) handlePositions(w http.ResponseWriter, r *http.Request) {
	account, ok := s.require(w, r)
	if !ok {
		return
	}
	pos, err := s.eng.Positions(r.Context(), account)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	out := make([]positionResp, 0, len(pos))
	for _, p := range pos {
		row := positionResp{
			Symbol: p.Symbol, Qty: p.Qty.String(), Reserved: p.ReservedQty.String(),
			CostBasis: p.CostBasis.String(), Realized: p.RealizedPnL.String(),
		}
		if p.HasMark {
			row.Mark, row.Unrealized = p.MarkPrice.String(), p.UnrealizedPnL.String()
		}
		out = append(out, row)
	}
	writeJSON(w, http.StatusOK, map[string]any{"positions": out})
}

// require resolves the caller or writes a 401.
func (s *Server) require(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, ok := accountOf(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "NOT_SIGNED_IN",
			"sign in first; see GET /auth/status for the available methods")
		return uuid.Nil, false
	}
	return id, true
}

func qtyString(v int64) string   { return money.Qty(v).String() }
func priceString(v int64) string { return money.Minor(v).String() }
