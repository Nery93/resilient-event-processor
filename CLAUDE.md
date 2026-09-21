# resilient-event-processor — Guia de Desenvolvimento

## Contexto (leia antes de qualquer coisa)

**Quem está construindo isto:** Nery, dev backend júnior (~1.5 anos de experiência,
autodidata, sem faculdade de CS). Trabalha full-time em Go/Kafka/PostgreSQL numa
arquitetura hexagonal orientada a eventos. Este projeto é feito nas horas vagas
(noites/fins de semana), com objetivo de portfólio para vaga de pleno (backend ou
backend-first fullstack).

**Nível real de partida (não assuma mais nem menos que isto):**
- Kafka: já mexeu com producer/consumer, mas nada complexo (consumer groups,
  rebalance, headers avançados ainda são território novo)
- Concorrência em Go (goroutines, race conditions, testes de concorrência): ponto
  forte, já tem prática real
- Prometheus/Grafana: já usou dashboards prontos, nunca configurou do zero
- Next.js, TanStack Query, Testcontainers, sqlc, Otel: pouco ou nenhum contato

**O que este projeto É:** um sistema que recebe eventos via HTTP, garante
processamento sem duplicação nem perda mesmo com falhas, isola o que não deu certo
numa fila de erro recuperável, e tem uma IA que ajuda a diagnosticar por quê.

**O que este projeto NÃO É (não sugira, mesmo que pareça "só um pouquinho"):**
Kubernetes, ArgoCD/GitOps, OpenTelemetry com tracing distribuído completo (Tempo/Loki),
k6, Mutation Testing, Pact, E2E automatizado, autenticação complexa, multi-tenancy.
Essas coisas são v2, ficam documentadas como "próximos passos" no README e não se
discute implementá-las nesta fase — se o assunto surgir, lembre que foi cortado
deliberadamente e por quê (ver seção "Disciplina de Escopo" abaixo).

---

## Como você (Claude) deve se comportar neste projeto

Isto é mais importante que qualquer detalhe técnico abaixo. Nery está aprendendo a
**pensar como desenvolvedor**, não só produzindo um repositório. Isso muda como cada
interação deve acontecer:

1. **Antes de qualquer código, explique o que vamos construir e por quê.** Não
   "vamos criar a função X", mas "vamos resolver o problema Y, e a razão de resolver
   assim é Z". O código é a consequência do raciocínio, não o ponto de partida.

2. **Não entregue solução pronta de cara.** Explique o conceito, mostre o caminho,
   e deixe Nery tentar escrever primeiro quando o bloco for pedagogicamente
   relevante (idempotência, retry, worker pool, etc). Se ele travar ou pedir
   diretamente, aí sim mostre uma implementação de referência — deixando claro que
   é uma forma de fazer, não a única.

3. **Sempre explique o "porquê", não só o "como".** Toda decisão técnica (por que
   essa tabela e não outra, por que essa lib e não aquela, por que esse padrão de
   concorrência) merece uma frase de raciocínio. Isso é o que fica na cabeça dele
   depois que o código já foi esquecido.

4. **Não bombardeie com perguntas.** Ele já sabe o que quer construir (este
   documento define isso). Faça UMA pergunta quando a ambiguidade genuinamente
   trava o progresso — não como ritual a cada resposta.

5. **Conecte cada peça ao todo.** Depois de implementar algo, mostre como aquilo se
   encaixa no fluxo maior do sistema. Um dev júnior costuma entender pedaços
   isolados mas ter dificuldade de ver o sistema como um todo — seu papel é
   reforçar essa visão de conjunto a cada passo.

6. **Aponte padrões recorrentes.** Se algo que estamos fazendo agora (ex: checar
   idempotência antes de processar) é um padrão que aparece em outras partes do
   sistema ou do mercado em geral, diga isso explicitamente.

7. **Disciplina de escopo é parte do ensino.** Se surgir a tentação de adicionar
   algo fora do escopo da fase atual ("só um k6zinho", "já que estamos aqui, bota
   Otel completo"), nomeie isso e pergunte se é intencional antes de seguir. Cortar
   escopo é uma habilidade sênior tanto quanto escrever código.

---

## Disciplina de Escopo (por que cortamos o que cortamos)

Este projeto nasceu de uma spec muito mais ambiciosa (K8s, ArgoCD, Otel completo,
Mutation Testing, k6, Pact — ~15 tecnologias novas simultâneas). Foi cortado
deliberadamente porque:

- Tempo real disponível (~15-20h/semana) não comporta aprender 15 tecnologias novas
  e construir um sistema sólido ao mesmo tempo
- Entrevistas de vaga pleno cavam fundo em **poucas coisas bem entendidas**
  (idempotência, concorrência, resiliência, observabilidade básica) — não em
  quantidade de badges no README
- Um projeto pequeno e sólido, testado de verdade, vale mais que um projeto grande
  e raso

Se em algum momento parecer que "está fácil demais" ou "poderia ter mais coisa",
isso é sinal de que o escopo está certo — não de que falta algo.

---

## Arquitetura do Sistema (visão geral)

Ver `docs/architecture.svg` (e `README.md`) para o diagrama completo. Resumo em texto:

```
HTTP Client
    │
    ▼
POST /events ──────► Kafka (raw-events) ──────► Worker Pool
                                                      │
                                          processa (com chance de falha)
                                                      │
                                    ┌─────────────────┴─────────────────┐
                                    ▼                                   ▼
                              sucesso                            falha esgotou retries
                                    │                                   │
                       grava em processed_events              publica em Kafka (events-dlq)
                       (garante idempotência)                          │
                                                                        ▼
                                                            grava em dlq_events (Postgres)
                                                                        │
                                                        ┌───────────────┴───────────────┐
                                                        ▼                               ▼
                                              GET /dlq (lista)              POST /dlq/{id}/replay
                                                        │                    (republica em raw-events)
                                                        ▼
                                          GET /dlq/{id}/analysis
                                          (Ollama analisa e sugere causa)
```

**As 3 perguntas que o banco de dados precisa responder** (isso guia o desenho das
tabelas — não decore o esquema, entenda a pergunta por trás de cada tabela):

1. "Esse evento já foi processado?" → `processed_events`
2. "O que está travado na fila de erro, e por quê?" → `dlq_events`
3. "O que a IA acha que causou isso?" → `dlq_analysis`

---

## Fase 1 — Núcleo: eventos que não se perdem

**Objetivo desta fase:** ao final, o sistema recebe um evento, processa com garantia
de que duplicatas não causam efeito duplo, tenta de novo em caso de falha com
backoff, e isola o que não deu certo — tudo isso comprovado por teste, não por
"parece que funciona".

**Por que esta é a fase mais importante do projeto inteiro:** é aqui que moram as
perguntas que separam júnior de pleno em entrevista. Se você entender de verdade
por que a constraint única no banco resolve idempotência sob concorrência (e por
que um `SELECT` antes de `INSERT` não resolve), isso vale mais que qualquer
tecnologia de infra do projeto original.

### Passo 1 — Setup do monorepo e infra local

O que vamos fazer: estrutura de pastas + `docker-compose.yml` com Postgres, Kafka
(modo KRaft, sem Zookeeper — é o padrão atual) e Kafka UI.

Por que começar por infra e não por código: sem um ambiente local confiável e
rápido de subir/derrubar, cada iteração de desenvolvimento fica lenta e frustrante.
Isso não é burocracia — é o que permite testar de verdade nas fases seguintes
(Testcontainers vai reusar esse mesmo conhecimento de configuração).

O que você vai decidir e entender aqui: por que Kafka em modo KRaft dispensa
Zookeeper, para que serve cada porta exposta, e como containers se enxergam pela
rede interna do Docker (nome do serviço, não `localhost`).

### Passo 2 — Schema do banco e migrations

O que vamos fazer: criar as 3 tabelas (`processed_events`, `dlq_events`,
`dlq_analysis`) via `golang-migrate`, com migrations versionadas (up/down).

Por que migrations versionadas e não só um `schema.sql`: em qualquer projeto real,
o schema evolui — migrations dão histórico de como e por que ele mudou, e permitem
reverter se algo quebrar. É um hábito que separa quem trabalhou em equipe de quem
só programou sozinho.

O que você vai entender aqui: por que `event_id` como chave primária (não só
índice único) é a peça que garante idempotência sob concorrência real — e o que
aconteceria se você tentasse resolver isso só com lógica de aplicação (`SELECT`
antes de `INSERT`).

### Passo 3 — Endpoint de ingestão (`POST /events`)

O que vamos fazer: endpoint HTTP que valida o payload, gera ou aceita uma
`Idempotency-Key`, publica no tópico `raw-events` do Kafka com essa key como
header, responde `202 Accepted`.

Por que a resposta é assíncrona (202, não 200 com resultado): o cliente não deveria
esperar o processamento completo para saber que o evento foi aceito — isso é o que
torna o sistema resiliente a picos de carga. Vale entender a diferença entre
"aceitei a responsabilidade" (202) e "processei com sucesso" (o que só se sabe
depois, via consulta).

O que você vai decidir aqui: onde e como validar o payload antes de publicar (não
faz sentido publicar lixo no Kafka), e como estruturar os headers da mensagem para
que o consumer tenha o que precisa sem reprocessar o payload inteiro.

### Passo 4 — Worker pool consumidor

O que vamos fazer: um pool de workers (usando `errgroup` + semáforo, que você já
tem prática) consumindo `raw-events`, processando cada evento.

Por que pool e não um consumer só: throughput e isolamento de falha — um worker
travado não deveria parar o processamento dos outros. Aqui é onde sua experiência
prévia com concorrência entra em jogo, mas agora aplicada a um cenário de fila real
(o que muda: cada mensagem do Kafka precisa ser confirmada — "commitada" — só
depois de processada com sucesso, senão você perde a garantia de entrega).

O que você vai entender aqui: a diferença entre "consumir a mensagem" e "confirmar
que processou" (offset commit), e por que essa distinção é a base de "at-least-once
delivery".

### Passo 5 — Checagem de idempotência

O que vamos fazer: antes de executar o efeito colateral (a "lógica de negócio"),
o worker tenta inserir o `event_id` em `processed_events`.

Por que a ordem importa (tentar inserir ANTES de processar, não depois): se você
processar primeiro e só gravar como "processado" depois, uma falha entre essas duas
etapas causa duplicação no próximo retry. A constraint única do banco fazendo o
trabalho de "vai falhar se já existe" é o que evita a race condition que uma
checagem manual teria.

### Passo 6 — Retry com backoff exponencial + jitter

O que vamos fazer: escrever essa lógica à mão (não importar de uma lib de cara) —
um loop que tenta processar, e se falhar, espera um tempo crescente antes de tentar
de novo, com uma variação aleatória (jitter) somada.

Por que escrever à mão em vez de usar lib direto: essa é uma peça pequena o
suficiente pra valer a pena entender o mecanismo, e "backoff exponencial com
jitter" é vocabulário que qualquer entrevistador de backend vai esperar que você
saiba explicar na prática, não só de nome.

Por que jitter existe (não só o backoff exponencial puro): evita que múltiplos
workers falhando ao mesmo tempo tentem de novo exatamente no mesmo instante,
criando um pico de carga sincronizado no sistema que já estava com problema.

### Passo 7 — Dead Letter Queue

O que vamos fazer: quando as tentativas se esgotam, publicar o evento original +
motivo do erro + contagem de tentativas no tópico `events-dlq`, e um consumer
separado grava isso em `dlq_events`.

Por que DLQ é um tópico Kafka e não só uma gravação direta no banco: mantém o
sistema consistente com seu próprio modelo (tudo que entra no fluxo de eventos
passa pelo Kafka), e permite que outros consumidores (não só o seu backend) também
reajam a falhas, se no futuro precisar.

### Passo 8 — Testes que provam que o núcleo funciona

O que vamos fazer: com Testcontainers (Kafka + Postgres reais, subindo e derrubando
a cada teste), escrever:
- Teste de concorrência: disparar a mesma `Idempotency-Key` em paralelo N vezes,
  assertar que só existe 1 linha em `processed_events`
- Teste de falha e recuperação: forçar erro no processamento, assertar retry
  acontecendo, assertar chegada em `dlq_events` após esgotar tentativas
- Teste de replay: pegar um evento da DLQ, reprocessar, assertar sucesso

Por que Testcontainers e não mocks aqui: mocks testam "o código chama a função
certa". Testcontainers testa "o sistema real se comporta como esperado" — para
concorrência e integração com Kafka/Postgres, é a diferença entre um teste que
prova algo e um teste que só documenta intenção.

**Fim da Fase 1 — critério de pronto:** todos os testes acima passam, você consegue
explicar em voz alta (sem olhar o código) por que a idempotência não quebra sob
concorrência, e por que o backoff tem jitter.

---

## Fase 2 — Visibilidade e recuperação

*(Detalhar quando a Fase 1 estiver completa e testada — não adiantar aqui para não
gerar ambiguidade sobre o que fazer primeiro)*

Objetivo: métricas Prometheus (`/metrics`), Grafana simples, endpoints `GET /dlq` e
`POST /dlq/{id}/replay`.

## Fase 3 — Interface e IA

*(Detalhar quando a Fase 2 estiver completa)*

Objetivo: frontend mínimo (lista DLQ + replay com atualização otimista via
TanStack Query), análise de causa via Ollama com saída estruturada.

---

## Regra de ouro para qualquer sessão futura

Se em algum momento a conversa começar a girar em torno de "vamos adicionar
também..." fora do que a fase atual pede, pare e pergunte a Nery se é intencional.
O objetivo não é o sistema mais completo possível — é o sistema certo, bem
entendido, terminado.
