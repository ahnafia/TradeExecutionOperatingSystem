# Build.
FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Static binary: the runtime stage has no libc to link against.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/tradectl ./cmd/tradectl

# Run.
#
# Distroless rather than alpine: this process talks to Postgres and serves HTTP, and needs
# neither a shell nor a package manager to do it. Anything that would want a shell in here
# is something that should not be happening in here.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/tradectl /app/tradectl

# A ceiling for the garbage collector.
#
# Go's GC has no idea a container has a hard memory limit; left alone it will happily grow
# the heap until the kernel kills the process, which looks like a crash rather than memory
# pressure. GOMEMLIMIT makes it collect harder as it approaches the bound instead — the
# process gets slower under load rather than dying. Sized under a 512Mi instance; raise it
# with the instance.
ENV GOMEMLIMIT=400MiB

# The port a managed host injects. Overridden at runtime by $PORT.
EXPOSE 9464
USER nonroot:nonroot

# serve runs the whole engine: HTTP, the outbox relay, matching, outcome consumers, and
# the market makers that give the book something to trade against. It applies migrations
# on boot, so a fresh database needs no separate step.
ENTRYPOINT ["/app/tradectl"]
CMD ["serve"]
