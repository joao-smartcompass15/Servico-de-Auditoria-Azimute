# syntax=docker/dockerfile:1

# --- Etapa 1: compilar ------------------------------------------------------
FROM golang:1.27.3-alpine AS build
WORKDIR /src

# Copia só go.mod/go.sum primeiro: se não mudarem, o Docker reaproveita o
# cache do download das dependências e o build fica bem mais rápido.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 gera um binário estático, que roda numa imagem sem sistema operacional.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/servico-auditoria ./cmd

# --- Etapa 2: imagem final mínima, sem shell e sem root ----------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/servico-auditoria /servico-auditoria
# Catálogo de ações embutido na imagem. Para trocar sem rebuild, monte outro
# arquivo por volume e aponte CATALOGO_ACOES para ele.
COPY config/acoes.json /config/acoes.json
ENV CATALOGO_ACOES=/config/acoes.json
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/servico-auditoria"]
