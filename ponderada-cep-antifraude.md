# Ponderada: Complex Event Processing (CEP)

## Software escolhido

Antifraude do checkout do site shopper.com.br

### Contexto

Shopper é um mercado online: cliente compra pelo site ou app, sem contato presencial no momento da compra. Por não existir verificação humana no caixa, o antifraude é o sistema que decide, durante o checkout, se o pedido segue direto, passa por verificação extra, ou é barrado, cruzando sinais do pedido (itens, valor, dispositivo, endereço) pra separar cliente legítimo de fraude sem travar a operação com falso positivo.

### Conceito de CEP que entendi com base nos autoestudos

Entendimento: eventos simples A e B acontecem, o motor de CEP os processa em tempo real e, ao encontrar um padrão entre eles, gera um evento complexo C, derivado dos dois primeiros. Isso dá ao sistema o poder de reagir à fraude "ao vivo", durante o checkout, em vez de descobrir o problema depois, numa análise em lote no fim do dia.

Aplicando ao meu exemplo: [E01](#e01) (item de risco) e [E02](#e02) (valor atualizado) são o A e o B. O motor de CEP aplica uma regra de conjunção sobre os dois, escopados ao mesmo pedido, enquanto o pedido está em `Criado`, e gera [E10](#e10) (compra suspeita), que é o C. A reação (pedir verificação extra ou negar) acontece enquanto o pedido ainda está aberto, porque o processamento é sobre o stream, não sobre um relatório do dia seguinte.

#### Critério de classificação usado na tabela de eventos

| Classificação | Definição | Teste de inclusão |
|---|---|---|
| Simples | Fato atômico, observado direto na fonte | Existe sozinho, sem precisar correlacionar com nenhum outro evento |
| Complexo | Evento derivado | Só existe como resultado de aplicar um operador de correlação (conjunção, janela deslizante, contagem/sequência, ausência) sobre 2 ou mais eventos simples, ou sobre a falta de um deles |
| Negócio | Representa um fato do processo de compra/pagamento | Relevante pro domínio em si, independente de qual tecnologia processa ele |
| Técnico | Se origina de um componente de infraestrutura/plataforma (fingerprinting, gateway, broker) | Só entra na modelagem se alimentar uma decisão do motor de fraude, virando input de um evento complexo ou mudando uma ação do sistema. Evento técnico que não afeta nenhuma decisão de negócio (ex: métrica genérica de saúde do broker) fica fora do escopo, é operação da plataforma Kafka, não evento do domínio de antifraude |

## 1. Tabela de eventos

Os 13 eventos abaixo foram levantados a partir do fluxo de checkout descrito no contexto e classificados segundo o critério acima. Todo evento carrega `orderId` e `sessionId` (ver diagrama 1), as correlações usam a chave que faz sentido pro caso: `orderId` pra eventos do mesmo pedido, `sessionId` pra eventos que podem repetir antes do pedido fechar (confirmação, biometria), `clienteId` pra correlação entre pedidos do mesmo cliente.

| ID | Evento | Descrição | Categoria | Tipo | Justificativa técnica |
|---|---|---|---|---|---|
| <a id="e01"></a>E01 | Item de categoria de risco adicionado ao carrinho | Cliente adiciona item de categoria álcool, carne ou churrasco | Simples | Negócio | Fato atômico do domínio, gerado direto pela ação do cliente, sem depender de correlação com outro evento |
| <a id="e02"></a>E02 | Valor total do pedido atualizado | Soma do carrinho muda a cada item adicionado ou removido | Simples | Negócio | Estado observável do pedido no instante da emissão, sem agregação sobre janela de tempo |
| <a id="e03"></a>E03 | Endereço de entrega diferente do histórico | Endereço do pedido não bate com os endereços recorrentes do cliente | Simples | Negócio | Comparação direta contra cadastro, sem janela temporal nem múltiplos eventos |
| <a id="e04"></a>E04 | Tentativa de pagamento | Cliente inicia a cobrança do pedido | Simples | Negócio | Marco único do fluxo de checkout |
| <a id="e05"></a>E05 | Cliente confirma ou erra valor cobrado | Cliente informa o valor que acha que foi cobrado | Simples | Negócio | Entrada única do usuário, sem dependência de eventos anteriores da mesma categoria |
| <a id="e06"></a>E06 | Scan facial solicitado | Sistema pede verificação biométrica antes de concluir a compra | Simples | Negócio | Ação disparada, não correlação |
| <a id="e07"></a>E07 | Resultado do scan facial (match ou no-match) | Retorno da verificação biométrica | Simples | Negócio | Resultado atômico de uma única chamada ao serviço de biometria |
| <a id="e08"></a>E08 | Novo dispositivo detectado | Fingerprint do device diverge do histórico do cliente | Simples | Técnico | Produzido por componente de infraestrutura (device fingerprinting), não por ação de domínio, mas alimenta direto a correlação de [E11](#e11) |
| <a id="e09"></a>E09 | Timeout ou falha na integração com gateway de pagamento | Erro de rede/infra na chamada ao gateway | Simples | Técnico | Usado pelo processador de [E12](#e12) pra excluir a tentativa do contador de fraude: falha do gateway não é a mesma coisa que fraudador testando cartão, sem essa exclusão o contador gera falso positivo |
| <a id="e10"></a>E10 | Compra suspeita | Valor do pedido acima do limite e item de categoria de risco no mesmo pedido, enquanto o pedido está em `Criado`, antes da tentativa de pagamento (`E04`) — nunca em `EmAnalise`, que só existe depois de `E04` (ver diagrama 3) | Complexo | Negócio | Conjunção (AND) de [E01](#e01) e [E02](#e02) escopada ao mesmo `orderId`. O que faz isso ser correlação de CEP e não um `if` disfarçado: [E01](#e01) e [E02](#e02) são publicados por partes distintas do checkout e não chegam em ordem garantida, o motor mantém um "partial match" (retém o primeiro evento até o par completar), igual pattern matching de motores CEP tipo Esper. Esse partial match tem TTL igual ao timeout de carrinho abandonado da loja: se o pedido nunca sai de `Criado` dentro desse prazo, o estado retido expira e é descartado, evitando acúmulo indefinido no state store |
| <a id="e11"></a>E11 | Possível conta comprometida | [E08](#e08), [E03](#e03) e valor alto ([E02](#e02)) dentro de uma janela curta (ex: 10 min), mesmo `clienteId` | Complexo | Negócio | Conjunção com janela deslizante sobre 3 eventos de origens distintas, a coincidência na janela é o que importa, não a ordem. [E03](#e03) e [E02](#e02) nascem chaveados por `orderId`, precisam de repartição pra `clienteId` antes do join (detalhe no diagrama 2), já que o mesmo cliente pode ter feito o pedido em sessões diferentes |
| <a id="e12"></a>E12 | Padrão de teste de cartão | Múltiplas tentativas de [E04](#e04) falhando dentro do mesmo `orderId`, antes do pedido fechar, líquido de exclusões de [E09](#e09) | Complexo | Negócio | Contagem com threshold sobre repetição do mesmo tipo de evento, escopada ao mesmo pedido (mesma chave de [E04](#e04)/[E09](#e09), sem repartição necessária, ao contrário de [E11](#e11)) |
| <a id="e13"></a>E13 | Decisão tomada sem sinal de dispositivo | Janela de correlação de [E11](#e11) expira sem [E08](#e08) chegar, qualquer que seja o motivo (latência, falha ou serviço fora do ar) | Complexo | Negócio | Padrão de ausência de verdade: nasce só da expiração do `AgendadorDeJanela` monitorando a janela (ver diagrama 1), não depende de nenhum sinal do serviço de origem. Se o Device Fingerprinting travar de vez e não emitir nada, o timer expira do mesmo jeito e [E13](#e13) dispara, é isso que diferencia ausência real de "reação a um evento de erro" |

### Cobertura das 4 combinações simples/complexo × técnico/negócio

Das 4 combinações possíveis entre as duas dimensões da tabela acima, a modelagem cobre 3: simples-negócio ([E01](#e01)-[E07](#e07), exceto E08/E09), simples-técnico ([E08](#e08), [E09](#e09)), complexo-negócio ([E10](#e10), [E11](#e11), [E12](#e12), [E13](#e13)). Não existe complexo-técnico nesta versão, de propósito: os únicos eventos técnicos do domínio ([E08](#e08), [E09](#e09)) entram como **insumo** de correlação, nunca como **saída** dela — o motor de CEP aqui só produz evento complexo pra alimentar decisão de negócio (compra suspeita, conta comprometida, teste de cartão), nunca uma decisão de infraestrutura. Um evento complexo-técnico existiria, por exemplo, se o motor correlacionasse repetições de [E09](#e09) entre pedidos distintos na mesma janela pra gerar um alerta de instabilidade do gateway — isso é operação de plataforma (SRE/observabilidade), não decisão de antifraude, por isso fica fora do escopo do domínio escolhido (ver critério de tipo técnico na tabela acima: só entra se alimentar decisão de negócio).

## 2. Modelagem estática e dinâmica (UML)

Seis diagramas cobrem os fluxos: dois estáticos (estrutura e arquitetura) e quatro dinâmicos (ciclo de vida do pedido e três sequências, uma por mecanismo de correlação distinto da tabela de eventos).

### 2.1 Modelagem estática

#### Diagrama 1: diagrama de classes, estrutura de domínio

Modela as entidades do pedido e a hierarquia de eventos. `EventoComplexo` tem duas formas: a comum, que agrega os eventos simples presentes que correlacionou (via a associação de agregação com `EventoSimples`, cardinalidade `0..*` cobrindo o caso vazio), e `EventoDeAusencia` (caso de [E13](#e13)), que nasce da falta de um evento esperado dentro de uma `JanelaDeCorrelacao`, monitorada por um `AgendadorDeJanela` que dispara a avaliação quando a janela expira sem o sinal chegar. `RegraDeCorrelacao` não carrega mais `janela: Duration` genérico na interface, porque nem toda implementação usa duração (`RegraConjuncao`, de [E10](#e10), é escopada por estado do pedido, não por tempo), cada implementação concreta declara o atributo que faz sentido pra ela.

```mermaid
classDiagram
    class Pedido {
        +orderId: string
        +clienteId: string
        +valorTotal: decimal
        +status: StatusPedido
    }
    class StatusPedido {
        <<enumeration>>
        Criado
        EmAnalise
        AvaliandoHistorico
        PendenteCentavos
        PendenteBiometria
        Aprovado
        Negado
    }
    class ItemPedido {
        +sku: string
        +categoria: Categoria
        +quantidade: int
    }
    class Categoria {
        <<enumeration>>
        ALCOOL
        CARNE_CHURRASCO
        OUTROS
    }
    class Cliente {
        +clienteId: string
        +enderecosHistorico: string[]
        +dispositivosHistorico: string[]
    }
    class EventoAntifraude {
        <<abstract>>
        +eventoId: string
        +timestamp: datetime
        +orderId: string
        +sessionId: string
        +clienteId: string
    }
    class EventoSimples
    class EventoComplexo
    class EventoDeAusencia {
        +eventoEsperado: string
    }
    class JanelaDeCorrelacao {
        +chave: string
        +inicio: datetime
        +duracao: Duration
    }
    class AgendadorDeJanela {
        +monitorar(janela: JanelaDeCorrelacao)
        +aoExpirar(janela: JanelaDeCorrelacao)
    }
    class RegraDeCorrelacao {
        <<interface>>
        +avaliar(eventos: EventoSimples[]) bool
    }
    class RegraConjuncao {
        +escopo: StatusPedido
        +ttlPartialMatch: Duration
    }
    class RegraJanelaDeslizante {
        +janela: Duration
    }
    class RegraContagem {
        +janela: Duration
        +threshold: int
    }
    class RegraAusencia {
        +janela: Duration
        +eventoEsperado: string
    }
    class AcaoAntifraude {
        <<interface>>
        +executar(pedido: Pedido, cliente: Cliente)
    }
    class CobrancaCentavos
    class ScanFacial
    class NegarPedido
    class BloquearConta

    Pedido "1" *-- "1..*" ItemPedido : composição
    ItemPedido --> Categoria
    Pedido --> Cliente
    Pedido --> StatusPedido
    RegraConjuncao --> StatusPedido : escopo
    EventoAntifraude <|-- EventoSimples
    EventoAntifraude <|-- EventoComplexo
    EventoComplexo <|-- EventoDeAusencia
    EventoDeAusencia --> JanelaDeCorrelacao
    AgendadorDeJanela --> JanelaDeCorrelacao : monitora
    RegraAusencia --> AgendadorDeJanela : assina expiração
    RegraJanelaDeslizante --> AgendadorDeJanela : abre e assina a mesma janela
    EventoComplexo --> RegraDeCorrelacao
    EventoComplexo "1" o-- "0..*" EventoSimples : agregação, correlaciona
    EventoComplexo --> AcaoAntifraude : dispara
    RegraDeCorrelacao <|.. RegraConjuncao
    RegraDeCorrelacao <|.. RegraJanelaDeslizante
    RegraDeCorrelacao <|.. RegraContagem
    RegraDeCorrelacao <|.. RegraAusencia
    AcaoAntifraude <|.. CobrancaCentavos
    AcaoAntifraude <|.. ScanFacial
    AcaoAntifraude <|.. NegarPedido
    AcaoAntifraude <|.. BloquearConta
```

`RegraConjuncao` implementa [E10](#e10), `RegraJanelaDeslizante` implementa [E11](#e11), `RegraContagem` implementa [E12](#e12), `RegraAusencia` implementa [E13](#e13) e é a única que produz um `EventoDeAusencia` em vez de um `EventoComplexo` comum, disparada só pelo `AgendadorDeJanela`, sem depender de nenhum evento simples de origem. `avaliar(eventos)` é chamado por ela com lista vazia, no callback de `aoExpirar`, a assinatura genérica da interface cobre esse caso sem precisar de método próprio. `RegraJanelaDeslizante` e `RegraAusencia` assinam o **mesmo** `AgendadorDeJanela`/`JanelaDeCorrelacao` (ver diagrama 5): a janela é aberta uma única vez esperando [E08](#e08), e o resultado bifurca conforme o sinal chega ([E11](#e11)) ou a janela expira antes ([E13](#e13)) — não são dois mecanismos de janela independentes.

`StatusPedido` é modelado como enum explícito porque é usado por dois lugares (`Pedido.status` e o escopo de `RegraConjuncao`), igual `Categoria`: os valores batem 1:1 com os estados do diagrama 3. `Cliente` não carrega mais um `status` próprio: nenhum diagrama dinâmico movimenta esse campo, `BloquearConta` (cenário [C3](#c3)) já é a ação concreta que cobre o bloqueio, sem precisar de um enum de estado do cliente que nunca é lido nem escrito em nenhuma sequência.

#### Diagrama 2: diagrama de arquitetura de streaming

Diagrama de arquitetura de streaming, complementar ao diagrama 1: mostra o roteamento real de tópico Kafka por evento, provando que a tabela 1 se sustenta em cima de um pipeline concreto. Só entram aqui os tópicos que alimentam alguma correlação de CEP ([E01](#e01), [E02](#e02), [E03](#e03), [E04](#e04), [E08](#e08), [E09](#e09)) — [E05](#e05) e [E07](#e07) são eventos simples de negócio já cobertos pela tabela 1, mas não participam de nenhuma `RegraDeCorrelacao`, então não têm papel a provar neste diagrama.

Cada serviço de origem é um producer que publica num tópico Kafka próprio por tipo de evento, com a chave de partição natural indicada em cada tópico. [E11](#e11) correlaciona por `clienteId`, mas `cart-value-updated` e `address-check` nascem chaveados por `orderId` (um cliente pode ter feito o pedido em sessões diferentes), então passam por `RK1` (repartição via `selectKey`) antes de chegar no processador.

[E12](#e12) correlaciona dentro do **mesmo `orderId`** (múltiplas tentativas de pagamento no mesmo pedido antes dele fechar), que já é a chave natural de `payment-attempt`/`gateway-failure`, então não precisa de repartição nenhuma.

[E13](#e13) não consome nenhum tópico: ele é puramente o timeout do `AgendadorDeJanela` que monitora a janela aberta por `P11` (ver diagrama 1). Isso é o que garante que E13 seja ausência de verdade, dispara mesmo se o Device Fingerprinting cair de vez e não publicar nada, não uma reação a um evento de erro específico.

```mermaid
flowchart LR
    subgraph Producers["Producers"]
        A1[Checkout Service]
        A2[Payment Gateway]
        A3[Device Fingerprinting]
    end

    subgraph Topicos["Kafka: chave de partição natural indicada em cada tópico — só tópicos que alimentam correlação"]
        T1[(cart-item-added, E01, chave=orderId)]
        T2[(cart-value-updated, E02, chave=orderId)]
        T3[(address-check, E03, chave=orderId)]
        T4[(payment-attempt, E04, chave=orderId)]
        T6[(device-fingerprint, E08, chave=clienteId)]
        T8[(gateway-failure, E09, chave=orderId)]
    end

    RK1[Repartição: selectKey por clienteId]

    subgraph CEPSingle["Motor de CEP"]
        P10[RegraConjuncao -> E10]
        P11[RegraJanelaDeslizante -> E11]
        P13[RegraAusencia -> E13, timer]
        P12[RegraContagem -> E12]
    end

    S1[(fraud-complex-events)]

    subgraph Consumers["Consumer de decisão"]
        D1[Serviço de decisão]
    end

    A1 --> T1
    A1 --> T2
    A1 --> T3
    A1 --> T4
    A2 --> T8
    A3 --> T6

    T1 --> P10
    T2 --> P10
    T6 --> P11
    T3 --> RK1
    T2 --> RK1
    RK1 --> P11
    P11 -.->|monitora janela, timeout via AgendadorDeJanela| P13
    T4 --> P12
    T8 --> P12

    P10 --> S1
    P11 --> S1
    P13 --> S1
    P12 --> S1

    S1 --> D1
```

### 2.2 Modelagem dinâmica

#### Diagrama 3: diagrama de estados, ciclo de vida do pedido

Representa como o pedido transita entre estados. `E10`, `E11` e `E13` saem de `Criado`, antes de `E04` (tentativa de pagamento) — os três são avaliados enquanto o cliente ainda monta o carrinho (diagramas 4 e 5). `E10` leva a `AvaliandoHistorico`, corresponde ao [C2](#c2); `E11`/`E13` correspondem ao [C1](#c1); `E12` só ocorre depois de `EmAnalise`, corresponde ao [C3](#c3). A transição `E12, BloquearConta` deixa explícito que quem move o pedido pra `Negado` é a ação `BloquearConta`, não `NegarPedido` (classes distintas de `AcaoAntifraude` no diagrama 1). Confirmação de centavos errada e biometria reprovada não viram evento complexo nesta versão: caem no fluxo padrão de `PendenteCentavos`.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Criado
    Criado --> AvaliandoHistorico : E10
    Criado --> PendenteBiometria : E11
    Criado --> PendenteBiometria : E13
    Criado --> EmAnalise : E04
    EmAnalise --> Aprovado : AvaliacaoConcluidaSemComplexo
    EmAnalise --> Negado : E12, BloquearConta
    AvaliandoHistorico --> Aprovado : HistoricoCompativel
    AvaliandoHistorico --> Negado : HistoricoIncompativel
    PendenteCentavos --> Aprovado : E05Correto
    PendenteBiometria --> Aprovado : E07Match
    PendenteBiometria --> PendenteCentavos : E07NoMatch
    Aprovado --> [*]
    Negado --> [*]
```

#### Diagrama 4: diagrama de sequência, combo suspeito até decisão por histórico (cenário C2)

Mostra como dois eventos simples do mesmo pedido são correlacionados pelo motor de CEP em [E10](#e10) via partial match (o processador retém o primeiro evento que chegar até o par completar), e como a decisão final consulta o histórico do cliente antes de agir, evitando negar um cliente recorrente. O histórico de compra consultado aqui vive no sistema de gestão de pedidos, fora da arquitetura de streaming do antifraude, é uma consulta síncrona a outro bounded context, não parte do pipeline de CQRS descrito na seção 2.3.

```mermaid
sequenceDiagram
    participant Cliente
    participant Checkout as Checkout Service
    participant Kafka
    participant CEP as Motor de CEP
    participant Decisao as Serviço de decisão
    participant Historico as Histórico do cliente (outro sistema)

    Cliente->>Checkout: adiciona item de categoria de risco
    Checkout->>Kafka: publica E01
    CEP->>CEP: retém E01, aguardando par (partial match)
    Cliente->>Checkout: valor total sobe do limite
    Checkout->>Kafka: publica E02
    Kafka->>CEP: consome E02, completa o par com E01 retido, mesmo orderId
    CEP->>CEP: avalia conjunção (valor alto AND categoria de risco)
    CEP->>Kafka: publica E10, compra suspeita
    Kafka->>Decisao: consome E10
    Decisao->>Historico: consulta síncrona ao padrão de compra do cliente
    Historico-->>Decisao: histórico compatível ou não
    alt histórico compatível, cliente recorrente
        Decisao->>Checkout: aprova pedido
    else histórico incompatível, cliente novo com esse padrão
        Decisao->>Checkout: nega pedido
    end
    Checkout->>Cliente: comunica resultado
```

#### Diagrama 5: diagrama de sequência, correlação multi-sinal, ausência de sinal e scan facial (cenário C1)

Mostra três eventos simples de origens diferentes (dispositivo, endereço, valor) sendo correlacionados numa janela deslizante, resultando em [E11](#e11). Inclui o caminho alternativo em que o dispositivo não responde a tempo: o `AgendadorDeJanela` detecta a expiração por conta própria, sem depender de nenhum sinal do Device Fingerprinting, e o motor gera [E13](#e13) (ausência de sinal) em vez de travar esperando, seguindo pro mesmo fallback conservador.

```mermaid
sequenceDiagram
    participant Device as Device Fingerprinting
    participant Cliente
    participant Checkout as Checkout Service
    participant Kafka
    participant CEP as Motor de CEP
    participant Agendador as Agendador de janela
    participant Decisao as Serviço de decisão
    participant Biometria as Serviço de biometria

    Cliente->>Checkout: informa endereço diferente do histórico
    Checkout->>Kafka: publica E03
    Cliente->>Checkout: valor total atualizado, alto
    Checkout->>Kafka: publica E02
    Kafka->>CEP: consome E03 e E02, mesmo clienteId
    CEP->>Agendador: abre janela de 10 min aguardando E08
    alt E08 chega dentro da janela
        Device->>Kafka: publica E08, novo dispositivo
        Kafka->>CEP: consome E08
        CEP->>CEP: avalia conjunção com janela deslizante
        CEP->>Kafka: publica E11, possível conta comprometida
    else janela expira sem E08, dispositivo pode até ter travado por completo
        Agendador->>CEP: notifica expiração da janela
        CEP->>Kafka: publica E13, decisão sem sinal de dispositivo
    end
    Kafka->>Decisao: consome E11 ou E13
    Decisao->>Checkout: solicita scan facial
    Checkout->>Cliente: pede scan facial, E06
    Cliente->>Biometria: realiza scan
    Biometria->>Kafka: publica E07, resultado
    Kafka->>Decisao: consome E07
    alt match
        Decisao->>Checkout: aprova pedido
    else no-match
        Decisao->>Checkout: fallback pra cobrança de centavos
    end
    Checkout->>Cliente: comunica resultado
```

#### Diagrama 6: diagrama de sequência, padrão de teste de cartão até bloqueio (cenário C3)

Mostra uma sequência de tentativas de pagamento pequenas e rápidas sendo contadas pelo motor de CEP até ultrapassar o limite, gerando [E12](#e12) e o bloqueio preventivo da conta. Uma falha de gateway ([E09](#e09)) no meio da sequência é descontada da contagem. Cada tentativa carrega um `eventoId` idempotente: Kafka garante entrega at-least-once por padrão, então se o producer reenviar a mesma tentativa, o processador de contagem reconhece o `eventoId` repetido e não conta duas vezes — pelo mesmo motivo, um atraso de rede que chega depois do grace period da janela de [E11](#e11) (diagrama 5) cai no caminho de [E13](#e13): ausência, não erro.

```mermaid
sequenceDiagram
    participant Fraudador
    participant Checkout as Checkout Service
    participant Gateway as Payment Gateway
    participant Kafka
    participant CEP as Motor de CEP
    participant Decisao as Serviço de decisão

    loop tentativas em sequência rápida
        Fraudador->>Checkout: tenta pagamento com cartão diferente
        Checkout->>Gateway: solicita cobrança pequena
        Gateway-->>Checkout: falha ou recusa
        Checkout->>Kafka: publica E04, com eventoId próprio
    end
    Gateway--)Kafka: publica E09, falha de gateway, 1 tentativa descontada
    Kafka->>CEP: consome sequência de tentativas, mesmo orderId, dedup por eventoId, líquido de E09
    CEP->>CEP: avalia contagem maior ou igual a N, janela curta
    CEP->>Kafka: publica E12, padrão de teste de cartão
    Kafka->>Decisao: consome E12
    Decisao->>Checkout: bloqueia conta
```

### 2.3 Event Sourcing e CQRS aplicados à arquitetura

**Event Sourcing, com a ressalva certa**: os tópicos de evento (`cart-item-added`, `device-fingerprint`, `fraud-complex-events` etc) são um log append-only, útil pra auditoria (reconstruir "o que aconteceu com esse pedido" replayando o stream daquele `orderId`). Isso não é Event Sourcing pleno enquanto `Pedido.status` (diagrama 1) continua sendo um campo mutável escrito diretamente pelos serviços de decisão: Event Sourcing de verdade exigiria que `status` fosse uma projeção, recalculada aplicando uma função de fold sobre o log de eventos daquele `orderId`, nunca escrita direto. Com a arquitetura atual, o que existe é log de auditoria com potencial de virar Event Sourcing pleno, não é a mesma coisa, e o documento não afirma mais que é.

**CQRS, com escopo honesto**: o lado de escrita é a ingestão via Kafka e os processadores de correlação (`P10`, `P11`, `P12`, `P13`), otimizado pra throughput, nunca consultado diretamente pelo Checkout. O lado de leitura é o consumer de decisão (`Serviço de decisão`, diagrama 2), que reage ao mesmo stream sem escrever nele. Esta versão do documento não materializa um read model dedicado (um índice cross-entity chegou a ser modelado numa revisão anterior e foi cortado por não ter cenário de negócio próprio), então o que sustenta CQRS aqui é a separação escrita/leitura em si, não uma estrutura de dados própria. O "histórico de compra do cliente" (diagrama 4) não conta como exemplo: vive no sistema de gestão de pedidos e é consultado de forma síncrona, integração externa, não o lado de leitura deste pipeline.

## 3. Cenários de negócio

#### Critério usado pra definir um cenário

| Critério | O que exige |
|---|---|
| Ancorado num evento real da tabela 1 | O evento de entrada é um ID específico ([E01](#e01) a [E13](#e13)) ou a ausência explícita de um evento complexo, nunca uma situação hipotética solta |
| Ação mapeada numa classe concreta | A ação disparada corresponde a uma das implementações de `AcaoAntifraude` do diagrama 1 (`CobrancaCentavos`, `ScanFacial`, `NegarPedido`, `BloquearConta`), não uma descrição vaga nova |
| Ganho sobre a transação, não sobre detecção em geral | O benefício declarado precisa ser sobre a transação em si (menos fricção, decisão mais rápida, bloqueio antes da perda), e não um argumento genérico de "detecta mais fraude" |
| Mecanismo de correlação real por trás | O evento de entrada é gerado por uma `RegraDeCorrelacao` de verdade (ou pela ausência de qualquer uma), não uma validação simples disfarçada de CEP |

| ID | Cenário | Evento(s) de entrada | Ação disparada | Ganho de eficiência |
|---|---|---|---|---|
| <a id="c1"></a>C1 | Scan facial só com correlação multi-sinal | [E11](#e11) ou [E13](#e13) | `ScanFacial` (resultado: [E06](#e06)) | Biometria só é exigida quando os sinais convergem (ou o sinal de dispositivo está ausente), reduz atrito no checkout da maioria dos pedidos, que não geram nenhum dos dois |
| <a id="c2"></a>C2 | Negação de combo suspeito com exceção por histórico do próprio cliente | [E10](#e10) correlacionado com histórico de compra do mesmo cliente | `NegarPedido` no ramo incompatível; no ramo compatível não há `AcaoAntifraude` nenhuma, é o fluxo padrão de aprovação (ver nota) | Reduz falso positivo em cliente recorrente (ex: churrasco de família), mantendo o bloqueio pra cliente novo com o mesmo padrão |
| <a id="c3"></a>C3 | Bloqueio preventivo por teste de cartão | [E12](#e12) | Bloquear conta | Detecção em tempo real corta a fraude na 3ª/4ª tentativa, antes do cartão testado ser usado numa compra de valor alto |

Nota sobre [C2](#c2): dos 3 cenários, é o único onde nem toda ação do fluxo mapeia numa classe `AcaoAntifraude` — aprovar não é uma ação de antifraude no diagrama 1, é ausência de ação (o pedido segue seu curso normal sem intervenção do motor). O critério "ação mapeada numa classe concreta" da tabela acima vale só pra ação de fraude disparada, não pro caminho de aprovação, que por definição não precisa de uma classe própria.
