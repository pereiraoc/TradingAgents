# Trading Lab — motor de testes com dinheiro fictício + grupo "Trading" no Odysseus

Data: 2026-10-26 · Fork: `pereiraoc/TradingAgents` (base v0.7.0) · Integração: fork `pereiraoc/odysseus`

## 1. Objetivo

Testar o TradingAgents sem dinheiro real e operar tudo pela interface do Odysseus:
rodar análises, ler relatórios, rodar backtests, manter carteiras fictícias (ao vivo e
reencenando o passado) e agendar análises recorrentes.

**Nada aqui envia ordem a corretora.** Toda operação é simulada.

## 2. Divisão de responsabilidades

| Onde | O que é dono |
|---|---|
| **Fork TradingAgents** (`tradingagents/lab/`) | Toda a lógica: fila e trabalhador, carteiras fictícias, regra de tamanho, preço de execução, extrato, curva x índice, reencenação, backtest, listas de ações, relatórios. Expõe uma API HTTP. |
| **Odysseus** (painel fino) | Grupo "Trading" na barra lateral que chama a API do fork; lista de modelos e chaves (já são dele); relógio da agenda (Tarefas) e ligar o Qwen no Cookbook; notificações. |

Regra: se uma decisão de negócio (quanto comprar, a que preço, o que entra na fila) aparecer
no código do Odysseus, está no lugar errado.

## 3. Implantação

- Container irmão `tradingagents-lab` no compose do Odysseus, declarado em
  `odysseus/docker/pereiraoc.yml` (arquivo de deltas locais do fork), com
  `build: /data/projects/TradingAgents` (Dockerfile do fork) e comando
  `tradingagents lab serve --host 0.0.0.0 --port 7100`.
- Sem porta publicada no host: só a rede interna do compose (`http://tradingagents-lab:7100`).
- Dados em `/data/trading` (bind-mount em `/home/appuser/.tradingagents`), `chmod 700`:
  `lab.sqlite`, `logs/` (relatórios), `cache/`, `memory/`.
- O Qwen local do Cookbook é alcançado em `http://odysseus:8000/v1`.
- Atualizar o motor = `git pull` no fork + `docker compose build tradingagents-lab`.
- O venv instalado em `/data/projects/TradingAgents/.venv` (26/10) fica como CLI avulsa;
  não é usado pelo painel.

## 4. Motor (fork) — `tradingagents/lab/`

Módulos pequenos, cada um testável sozinho:

| Módulo | Responsabilidade |
|---|---|
| `store.py` | SQLite (`lab.sqlite`), migrações, acesso às tabelas. |
| `jobs.py` | Fila: enfileirar, pegar o próximo, marcar estado, retomar após reinício. |
| `worker.py` | Thread única que consome a fila; roda o grafo pelo mesmo caminho de *stream* da CLI (`begin_checkpoint`/`end_checkpoint`, checkpoint ligado), gravando um evento por nó concluído. |
| `llm.py` | Monta a config do TradingAgents a partir do modelo recebido (provider, base_url, modelo quick/deep, chave só em memória). |
| `sizing.py` | Converte decisão (nota + `position_sizing` do trader) em quantidade-alvo. |
| `fills.py` | Preço de execução: fechamento do primeiro pregão após a data da análise (yfinance). |
| `portfolio_book.py` | Carteira: aplica operações, valoriza, curva diária, índice de referência. |
| `replay.py` | Reencenação: para cada data da grade, analisa com a carteira daquele momento e aplica a operação. |
| `watchlists.py` | Listas de ações nomeadas (usadas pela agenda). |
| `api.py` | API HTTP (FastAPI + uvicorn, extra opcional `[lab]`). |

Comando novo: `tradingagents lab serve`.

### 4.1 Tabelas

- `jobs(id, kind[analysis|backtest], status[queued|running|done|failed|review],
  ticker, trade_date, analysts, model_json (sem chave), portfolio_id?, parent_id?, error,
  created_at, started_at, finished_at, report_dir, rating)`
- `job_events(job_id, ts, agent, phase[start|end], note)` — andamento ao vivo.
- `portfolios(id, name, currency, initial_cash, benchmark, mode[live|replay],
  replay_json?, weights_json, created_at)`
- `trades(id, portfolio_id, job_id, ticker, side, quantity, price, fill_date,
  rating, sizing_source[ai_percent|ai_quantity|ai_amount|default_weight], status[pending|filled], reason)`
- Caixa = `initial_cash` − compras + vendas preenchidas (calculado, sem tabela própria).
- `watchlists(id, name, tickers_json)`
- `backtests(id, tickers_json, start, end, every_days, analysts, model_json, status, summary_json)`

### 4.2 Chaves de API

A chave chega no corpo do pedido e é mantida **só em memória** no trabalhador, associada ao
job. Nunca vai para o SQLite, log ou linha de comando. Job que ficou na fila quando o
container reiniciou e precisa de chave volta como `failed` com o erro
"chave perdida no reinício — reenvie"; modelos locais sem chave são retomados normalmente.

### 4.3 Regra de tamanho (`sizing.py`)

Entrada: nota final do Portfolio Manager (Buy/Overweight/Hold/Underweight/Sell/REVIEW), texto
de sizing do trader, carteira atual, preço de referência.

- A **direção** vem sempre da nota do Portfolio Manager (é ela que o fork devolve como sinal).
- O **tamanho** vem do trader: `trader_investment_plan` no estado final é texto renderizado por
  `render_trader_proposal`; o campo é lido da linha `**Position Sizing**: ...`. Se o trader
  apontou outra direção que o PM (ex.: trader Buy, PM Underweight), o sizing dele é ignorado
  e vale o peso padrão da nota do PM.

1. `REVIEW` → nenhuma operação; job vai para `review` para o usuário decidir.
2. `Hold` → nenhuma operação.
3. `position_sizing` com percentual ("5% of portfolio") → peso-alvo = esse percentual do
   valor total da carteira.
4. Com quantidade ("200 shares") → quantidade-alvo direta; com valor ("$10,000") →
   valor-alvo.
5. Vago ou ausente → peso padrão da nota (padrão: Buy 10%, Overweight 7%, Underweight 3%,
   Sell 0%), editável por carteira.
6. Sell com sizing parcial ("trim 50%") → reduz a posição nessa fração.
7. Quantidade inteira (arredonda para baixo), limitada pelo caixa; sem alavancagem;
   **sem venda a descoberto na v1** (a posição nunca fica negativa).
8. Nota de alta nunca reduz a posição e nota de baixa nunca aumenta: o alvo é limitado
   pela posição atual. Quantidade/valor do trader são o *tamanho da ordem* (delta),
   percentual "of portfolio" é peso-alvo, percentual "of the position" em nota de baixa é
   a fração a vender.
9. O cálculo acontece **no preenchimento**, com o preço de execução e a carteira daquele dia.

`sizing_source` registra qual regra valeu, para auditoria no extrato.

### 4.4 Execução (`fills.py`)

Preço = fechamento do **primeiro pregão após** `trade_date`. Se esse pregão ainda não
aconteceu (carteira ao vivo), a operação fica `pending` e é preenchida quando o trabalhador
roda de novo ou num tique de 1 h. Uma moeda por carteira: ticker de outra moeda é recusado
na criação do job.

### 4.5 Reencenação (`replay.py`)

Carteira `replay` com período, intervalo e ações. Gera os jobs na ordem cronológica
(`parent_id` = carteira); cada job só roda depois do anterior aplicar sua operação, porque a
carteira daquele momento entra como contexto (`PortfolioContext`). Resultado: curva de valor
x índice no período, total de operações, acerto por nota.

### 4.6 Backtest

Reusa `tradingagents.backtest.run_backtest` (grade ações × datas, sem carteira) como **um job
`backtest`** na mesma fila; o `progress` dele vira eventos do job, e uma varredura interrompida
retoma ao rodar de novo (o próprio `run_backtest` pula células já feitas). O resumo
(`summarize(...).render()` + números por nota) é salvo em `summary_json`.

### 4.7 API HTTP

```
GET  /health                      versão do motor, fila, modelo ocupado
POST /jobs                        {ticker, date, analysts, model, portfolio_id?}
GET  /jobs?status=&limit=         lista
GET  /jobs/{id}                   detalhe + eventos
POST /jobs/{id}/retry             retoma do checkpoint
POST /jobs/{id}/resolve           decide um REVIEW (nota escolhida ou ignorar)
DELETE /jobs/{id}                 cancela se na fila
GET  /reports/{job_id}            HTML do relatório (report_html do fork)
GET  /reports/{job_id}/md         Markdown
GET/POST /portfolios              lista / cria (live ou replay)
GET  /portfolios/{id}             posições, caixa, valor, curva, índice, extrato
POST /portfolios/{id}/run         {tickers | watchlist_id, model} → enfileira análises nela
GET/POST/DELETE /watchlists
GET/POST /backtests, GET /backtests/{id}
```

Sem autenticação própria: só acessível pela rede interna do compose; o Odysseus faz a
autenticação do usuário antes de repassar.

## 5. Odysseus — grupo "Trading"

Mesmo padrão do Finanças e do Uso de IA (arquivos novos + ganchos mínimos, registrados em
`LOCAL_CHANGES.md` §8):

- `routes/trading_routes.py`: proxy autenticado `/api/trading/*` → `http://tradingagents-lab:7100`;
  `GET /api/trading/models` lista `model_endpoints` habilitados; ao criar job/run, resolve o
  endpoint escolhido (base_url, modelo, chave decifrada) e anexa ao corpo.
- `static/js/trading.js`, `static/css/zz-fork-trading.css`: seção `#trading-section` com abas
  **Analisar**, **Relatórios**, **Carteiras**, **Backtest**, **Agenda**; andamento por polling
  de `/jobs/{id}`.
- Ação `trading_run` registrada em `BUILTIN_ACTIONS`/`BUILTIN_ACTION_INFO` **a partir do módulo
  do painel** (sem editar `src/builtin_actions.py`). O agendador já repassa `task.prompt` como
  `prompt=` a qualquer ação e mapeia a autoridade para `scheduled__trading_run`, então a config
  vai em JSON no prompt: `{watchlist_id | tickers, portfolio_id?, endpoint_id, model,
  wait_model_min}`. Fluxo: espera o endpoint responder até `wait_model_min`; envia
  `POST /portfolios/{id}/run` (ou jobs avulsos); espera os jobs terminarem; devolve resumo
  ("PETR4 Buy, VALE3 Hold…"), que o agendador já notifica.
- **Ligar o Qwen não é feito de dentro da `trading_run`**: a autoridade dela não inclui o
  recurso `serve_model`. A aba **Agenda** cria um par de Tarefas pela API existente — uma
  `cookbook_serve` agendada (preset + `end_after_min`) com `then_task_id` apontando para a
  `trading_run` (sem agenda própria), que espera o modelo subir. As duas aparecem nas Tarefas
  do Odysseus. Com modelo remoto, só a `trading_run` agendada.
- Os relatórios são mostrados em `<iframe sandbox srcdoc>` (o Odysseus manda `X-Frame-Options:
  DENY` em toda rota, então não dá para iframar uma URL; `srcdoc` herda o CSP da página, que
  permite estilo inline e bloqueia script — o HTML do relatório é estático).
- Ganchos: `app.py` (registrar rotas), `static/index.html` (seção, CSS, script),
  `docker/pereiraoc.yml` (serviço `tradingagents-lab` + `TRADING_LAB_URL`).
- Sem motor respondendo: o painel mostra aviso com o comando de build.

## 6. Erros

| Situação | Comportamento |
|---|---|
| Processo/modelo/Yahoo falha | job `failed` com erro; botão "tentar de novo" retoma do checkpoint. |
| Nota ilegível | `review`; nenhuma operação até o usuário decidir. |
| Container reinicia com job `running` | volta para `queued` (ou `failed` se dependia de chave, §4.2). |
| Preço do pregão seguinte indisponível | operação `pending`, preenchida depois. |
| Moeda diferente da carteira | recusado ao enfileirar. |
| Qwen não sobe na tarefa agendada | `trading_run` desiste após `wait_model_min`, falha e notifica; nada é enfileirado. |
| Chat do Odysseus usando o Qwen ao mesmo tempo | permitido; as duas coisas ficam mais lentas (o servidor atende em sequência). |

## 7. Testes

Fork (`pytest`, LLM falso, preços falsos):
- `sizing`: percentual, quantidade, valor, vago, ausente, REVIEW, Hold, trim parcial, limite de caixa, short desligado.
- `fills`: pregão seguinte, fim de semana/feriado, pendente.
- `portfolio_book`: caixa, posições, valorização, curva x índice.
- `jobs`/`worker`: ordem FIFO, retomada após reinício, chave nunca persistida (varre o SQLite).
- `replay`: cadeia cronológica, carteira do momento passada como contexto.
- `api`: rotas principais com cliente de teste.

Odysseus: proxy (auth, injeção de chave só no corpo), ação `trading_run` com motor falso.

Ponta a ponta (com o usuário): uma análise numa data passada com o Qwen; uma reencenação
pequena (1 ação × 3 datas); uma tarefa agendada. Validação visual pelo navegador — nunca
compositor aninhado.

## 8. Fora do escopo

- Ordens reais ou conta de corretora (inclusive simulador de corretora).
- Ligar com o painel Finanças / investimentos reais (decidido: depois).
- Várias análises em paralelo (o Qwen local atende uma por vez).
