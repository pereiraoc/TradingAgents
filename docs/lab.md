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

## Radar

Quais ações valem uma análise agora. A coleta roda sozinha de hora em hora (`POST /radar/collect`
força) e não usa modelo:

| Fonte | O quê | Atraso |
|---|---|---|
| Bloomberg | manchetes dos RSS públicos (markets, politics, technology, economics) | minutos |
| Notícias sobre figuras | Google News sobre o que cada figura disse (lista editável, EN/PT) | minutos |
| Congresso EUA | compras/vendas declaradas (Bargo, sem chave, limite diário) | até ~45 dias |
| Executivos (SEC) | Form 4: compra no mercado (ou venda grande) da própria empresa | ~2 dias úteis |

`POST /radar/run {model}` (job `radar`): a IA lê os sinais de texto → (ação, direção, força); o
ranking soma os sinais dos últimos 7 dias com meia-vida de 3 dias, peso por fonte e bônus quando
fontes diferentes concordam. Se `config.portfolio_id` estiver definido, as `top_n` ações de alta
(pontuação ≥ `min_score`, moeda da carteira, sem repetir em `cooldown_days`) viram análises nessa
carteira fictícia — é assim que se mede se seguir o Radar dá dinheiro.

```
GET  /radar?days=7          ranking, sinais recentes, pendentes, config
POST /radar/config          {figures, sources, portfolio_id, top_n, min_score, cooldown_days}
POST /radar/collect         coleta agora
POST /radar/run             {model, api_key?, analysts?}
GET  /search?q=             símbolos do Yahoo (+ ".SA" para código da B3)
```

Uma análise cujo modelo não responde **espera na fila** (até 2 h) em vez de falhar.
