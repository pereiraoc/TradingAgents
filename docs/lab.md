# Trading Lab (fork pereiraoc)

Carteiras com **dinheiro fictício**, fila de análises, reencenação de períodos passados e
backtest, servidos por uma API HTTP. **Nunca envia ordem a corretora**: toda operação é uma
linha num SQLite local.

Design: `docs/superpowers/specs/2026-10-26-trading-lab-design.md`.

## Subir

```bash
pip install ".[lab]"
tradingagents lab serve --host 127.0.0.1 --port 7100
```

No desktop do pereiraoc ele roda como o container `tradingagents-lab` do compose do Odysseus
(`odysseus/docker/pereiraoc.yml`), visível só na rede interna, e é operado pelo grupo
**Trading** da barra lateral do Odysseus.

| Variável | Padrão | O quê |
|---|---|---|
| `TRADINGAGENTS_LAB_DIR` | `~/.tradingagents` | `lab.sqlite`, `reports/<job>/`, `logs/`, `cache/`, `memory/` |

## Como uma decisão vira operação

- **Direção**: a nota do Portfolio Manager (Buy, Overweight, Hold, Underweight, Sell).
- **Tamanho**: a linha `Position Sizing` do trader, se ele concorda com a direção:
  `"5% of portfolio"` = peso-alvo; `"200 shares"` / `"$10,000"` = tamanho da ordem;
  `"trim 50% of the position"` (nota de baixa) = fração a vender.
- Sem sizing legível: peso padrão da nota — Buy 10%, Overweight 7%, Underweight 3%, Sell 0%
  (editável por carteira em `weights`).
- Hold não opera; REVIEW (nota ilegível) espera você decidir (`POST /jobs/{id}/resolve`).
- Preço = **fechamento do primeiro pregão após** a data da análise; se ainda não existe, a
  operação fica pendente e é preenchida depois (o trabalhador tenta de hora em hora).
- Nota de alta nunca reduz posição; de baixa nunca aumenta; sem venda a descoberto; quantidade
  inteira limitada pelo caixa.

## Chaves de API

Chegam no corpo do pedido (`api_key`) e ficam **só em memória** até o job terminar. Se o
serviço reiniciar, jobs que precisavam de chave falham com "chave perdida no reinício —
reenvie" (`POST /jobs/{id}/retry` com a chave); jobs de modelo local voltam para a fila.

## API

```
GET  /health
POST /jobs                  {ticker, date, analysts?, model, api_key?, portfolio_id?}
GET  /jobs?status=&limit=
GET  /jobs/{id}             + events (agente a agente)
POST /jobs/{id}/retry       {api_key?}
POST /jobs/{id}/resolve     {rating | null}
DELETE /jobs/{id}           só na fila
GET  /reports/{id}          HTML    · GET /reports/{id}/md  Markdown
GET  /portfolios            · POST /portfolios {name, currency, initial_cash, benchmark?, weights?,
                              mode: live|replay, replay?: {tickers, start, end, every_days, analysts?, model, api_key?}}
GET  /portfolios/{id}       posições, caixa, valor, curva x índice, extrato
POST /portfolios/{id}/run   {tickers | watchlist_id, analysts?, model, api_key?, date?}
GET/POST /watchlists        {name, tickers} · DELETE /watchlists/{id}
GET/POST /backtests         {tickers, start, end, every_days, analysts?, model, api_key?} · GET /backtests/{id}
```

`model` = `{provider?: "openai_compatible", base_url, quick, deep?}`.
