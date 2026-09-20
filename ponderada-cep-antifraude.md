# Ponderada: Complex Event Processing (CEP)

## Software escolhido

Antifraude do checkout do site shopper.com.br

### Contexto

Shopper é um mercado online: cliente compra pelo site ou app, sem contato presencial no momento da compra. Por não existir verificação humana no caixa, o antifraude é o sistema que decide, durante o checkout, se o pedido segue direto, passa por verificação extra, ou é barrado, cruzando sinais do pedido (itens, valor, dispositivo, endereço) pra separar cliente legítimo de fraude sem travar a operação com falso positivo.

## Tabela de eventos

| ID | Evento | Descrição | Categoria | Tipo | Justificativa técnica |
|---|---|---|---|---|---|
| <a id="e01"></a>E01 | Item de categoria de risco adicionado ao carrinho | Cliente adiciona item de categoria álcool, carne ou churrasco | Simples | Negócio | Fato atômico do domínio, gerado direto pela ação do cliente, sem depender de correlação com outro evento |
| <a id="e02"></a>E02 | Valor total do pedido atualizado | Soma do carrinho muda a cada item adicionado ou removido | Simples | Negócio | Estado observável do pedido no instante da emissão, sem agregação sobre janela de tempo |
| <a id="e03"></a>E03 | Endereço de entrega diferente do histórico | Endereço do pedido não bate com os endereços recorrentes do cliente | Simples | Negócio | Comparação direta contra cadastro, sem janela temporal nem múltiplos eventos |
| <a id="e04"></a>E04 | Tentativa de pagamento | Cliente inicia a cobrança do pedido | Simples | Negócio | Marco único do fluxo de checkout |
| <a id="e05"></a>E05 | Cobrança de N centavos autorizada | Gateway autoriza microcobrança de verificação | Simples | Negócio | Resultado pontual de uma chamada ao gateway |
| <a id="e06"></a>E06 | Cliente confirma ou erra valor cobrado | Cliente informa o valor que acha que foi cobrado | Simples | Negócio | Entrada única do usuário, sem dependência de eventos anteriores da mesma categoria |
| <a id="e07"></a>E07 | Scan facial solicitado | Sistema pede verificação biométrica antes de concluir a compra | Simples | Negócio | Ação disparada, não correlação |
| <a id="e08"></a>E08 | Resultado do scan facial (match ou no-match) | Retorno da verificação biométrica | Simples | Negócio | Resultado atômico de uma única chamada ao serviço de biometria |
| <a id="e09"></a>E09 | Novo dispositivo detectado | Fingerprint do device diverge do histórico do cliente | Simples | Técnico | Produzido por componente de infraestrutura (device fingerprinting), não por ação de domínio, mas alimenta direto a correlação de [E14](#e14) |
| <a id="e10"></a>E10 | Timeout ou falha na integração com gateway de pagamento | Erro de rede/infra na chamada ao gateway | Simples | Técnico | Evento de sistema que afeta se a cobrança de [E05](#e05) chega a acontecer, tem consequência direta no fluxo de negócio |
| <a id="e11"></a>E11 | Falha na chamada ao serviço de biometria | Serviço de scan facial fica indisponível ou retorna erro | Simples | Técnico | Sem esse sinal, [E08](#e08) não pode ser emitido, o sistema precisa cair num fluxo alternativo (ex: repetir cobrança de centavos) em vez de travar o checkout |
| <a id="e12"></a>E12 | Latência alta na chamada ao serviço de device fingerprinting | [E09](#e09) demora além do aceitável pra chegar | Simples | Técnico | Se o sinal de dispositivo não chega dentro da janela de correlação de [E14](#e14), o sistema decide sem esse sinal, o que muda o resultado da avaliação de risco |
| <a id="e13"></a>E13 | Compra suspeita | Valor do pedido acima do limite e item de categoria de risco no mesmo pedido | Complexo | Negócio | Conjunção (AND) de [E01](#e01) e [E02](#e02) escopada ao mesmo `orderId`, não existe sem correlacionar os dois |
| <a id="e14"></a>E14 | Possível conta comprometida | [E09](#e09), [E03](#e03) e valor alto dentro de uma janela curta (ex: 10 min) | Complexo | Negócio | Conjunção com janela deslizante sobre 3 eventos de origens distintas, a coincidência na janela é o que importa, não a ordem |
| <a id="e15"></a>E15 | Padrão de teste de cartão | Múltiplas tentativas de [E04](#e04) falhando ou trocando de cartão em curto intervalo | Complexo | Negócio | Sequência com contagem (threshold sobre repetição do mesmo tipo de evento na mesma sessão) |
| <a id="e16"></a>E16 | Confirmação inconsistente | 2 ou mais ocorrências de [E06](#e06) com erro, na mesma sessão | Complexo | Negócio | Contagem com escopo de sessão, evento simples repetido vira gatilho só ao ultrapassar o threshold |
| <a id="e17"></a>E17 | Falha biométrica recorrente | 2 ou mais ocorrências de [E08](#e08) com no-match, na mesma sessão | Complexo | Negócio | Mesmo mecanismo de [E16](#e16), aplicado a outro evento simples de origem |
| <a id="e18"></a>E18 | Dispositivo ou endereço compartilhado entre múltiplas contas | Mesmo fingerprint ou endereço aparece em pedidos de contas de cliente diferentes numa janela curta | Complexo | Negócio | Correlação cross-entity: não é escopada a um único `orderId`/cliente, mas a um recurso compartilhado entre streams de clientes distintos, padrão típico de detecção de fraude em anel |

12 eventos simples (8 negócio, 4 técnico) e 6 complexos (todos negócio, por serem decisões derivadas).

## Cenários de negócio

| ID | Cenário | Evento(s) de entrada | Ação disparada | Ganho de eficiência |
|---|---|---|---|---|
| <a id="c1"></a>C1 | Cobrança de centavos em vez de bloqueio direto | 1 sinal de risco isolado (ex: [E03](#e03), sem outros sinais na janela) | [E05](#e05) | Evita negar venda legítima por 1 sinal fraco, resolve a ambiguidade com fricção mínima |
| <a id="c2"></a>C2 | Scan facial só com correlação multi-sinal | [E14](#e14) | [E07](#e07) | Biometria só é exigida quando 3 sinais convergem, reduz atrito no checkout da maioria dos pedidos, que não geram [E14](#e14) |
| <a id="c3"></a>C3 | Negação de combo suspeito com exceção por histórico do próprio cliente | [E13](#e13) correlacionado com histórico de compra do mesmo cliente | Negar pedido, ou liberar se o padrão já é recorrente pra esse cliente | Reduz falso positivo em cliente recorrente (ex: churrasco de família), mantendo o bloqueio pra cliente novo com o mesmo padrão |
| <a id="c4"></a>C4 | Bloqueio preventivo por teste de cartão | [E15](#e15) | Bloquear conta | Detecção em tempo real corta a fraude na 3ª/4ª tentativa, antes do cartão testado ser usado numa compra de valor alto |
| <a id="c5"></a>C5 | Escalonamento progressivo por confirmação inconsistente | [E06](#e06) repetido, virando [E16](#e16) | 1º erro: pedir nova confirmação. 2º erro ([E16](#e16)): negar ou exigir [E07](#e07) | Erro isolado de digitação não penaliza cliente legítimo, só o padrão repetido eleva a ação |
| <a id="c6"></a>C6 | Detecção de fraude coordenada entre contas | [E18](#e18) | Flag das contas envolvidas pra revisão, ou bloqueio em lote | Correlação entre clientes diferentes (não só dentro de 1 pedido) pega fraude organizada que nenhuma regra por pedido isolado detectaria |

## Diagramas UML

Seis diagramas cobrem os fluxos: dois estáticos (estrutura e arquitetura), um dinâmico de ciclo de vida, e três dinâmicos de sequência, um por mecanismo de correlação distinto da tabela de eventos.

### 1. Diagrama de classes: estrutura de domínio

Modela as entidades do pedido e a hierarquia de eventos: evento simples e complexo herdam de uma classe abstrata comum, o evento complexo agrega os eventos simples que correlaciona através de uma regra, e dispara uma ação que implementa uma interface comum.

```mermaid
classDiagram
    class Pedido {
        +orderId: string
        +clienteId: string
        +valorTotal: decimal
        +status: StatusPedido
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
    }
    class EventoSimples
    class EventoComplexo {
        +eventosOrigem: EventoSimples[]
    }
    class RegraDeCorrelacao {
        <<interface>>
        +janela: Duration
        +avaliar(eventos: EventoSimples[]) bool
    }
    class AcaoAntifraude {
        <<interface>>
        +executar(pedido: Pedido)
    }
    class CobrancaCentavos
    class ScanFacial
    class NegarPedido
    class BloquearConta

    Pedido "1" --> "many" ItemPedido
    ItemPedido --> Categoria
    Pedido --> Cliente
    EventoAntifraude <|-- EventoSimples
    EventoAntifraude <|-- EventoComplexo
    EventoComplexo --> RegraDeCorrelacao
    EventoComplexo "1" --> "many" EventoSimples : correlaciona
    EventoComplexo --> AcaoAntifraude : dispara
    AcaoAntifraude <|.. CobrancaCentavos
    AcaoAntifraude <|.. ScanFacial
    AcaoAntifraude <|.. NegarPedido
    AcaoAntifraude <|.. BloquearConta
```

### 2. Diagrama de componentes: arquitetura de streaming

Cada serviço de origem é um producer que publica num tópico Kafka próprio por tipo de evento. Os tópicos são particionados por `orderId` (ou `clienteId`, pros eventos que correlacionam entre pedidos como [E18](#e18)), garantindo que os eventos do mesmo pedido cheguem em ordem ao mesmo consumer, requisito pra correlação de janela funcionar. O motor de CEP é um consumer group que aplica os operadores (conjunção, janela deslizante, contagem) e publica o evento complexo resultante num tópico de saída, consumido pelos serviços de decisão.

```mermaid
flowchart LR
    subgraph Producers["Producers"]
        A1[Checkout Service]
        A2[Payment Gateway]
        A3[Device Fingerprinting]
        A4[Biometria Service]
    end

    subgraph Kafka["Kafka: tópicos particionados por orderId/clienteId"]
        T1[(cart-item-added)]
        T2[(payment-attempt)]
        T3[(device-fingerprint)]
        T4[(biometric-result)]
    end

    subgraph CEP["Motor de CEP: consumer group"]
        B1[Operador de conjunção]
        B2[Operador de janela deslizante]
        B3[Operador de contagem/sequência]
    end

    S1[(fraud-complex-events)]

    subgraph Consumers["Consumers de decisão"]
        C1[Serviço de decisão]
        C2[Notificação]
        C3[Time de risco]
    end

    A1 --> T1
    A2 --> T2
    A3 --> T3
    A4 --> T4
    T1 --> B1
    T2 --> B1
    T3 --> B2
    T4 --> B3
    B1 --> S1
    B2 --> S1
    B3 --> S1
    S1 --> C1
    S1 --> C2
    S1 --> C3
```

### 3. Diagrama de estados: ciclo de vida do pedido

Representa como o pedido transita entre estados conforme os eventos (simples e complexos) chegam. Cobre os seis cenários de negócio de forma genérica, já que todos resultam numa dessas transições.

```mermaid
stateDiagram-v2
    [*] --> Criado
    Criado --> EmAnalise: E04 tentativa de pagamento
    EmAnalise --> PendenteConfirmacaoCentavos: C1, sinal isolado
    EmAnalise --> PendenteBiometria: C2, E14 possível conta comprometida
    EmAnalise --> Negado: C4, E15 padrão de teste de cartão
    EmAnalise --> Aprovado: sem evento complexo
    EmAnalise --> RevisaoManual: C6, E18 fraude coordenada
    PendenteConfirmacaoCentavos --> Aprovado: E06 confirmado corretamente
    PendenteConfirmacaoCentavos --> PendenteConfirmacaoCentavos: E06 incorreto, 1a vez
    PendenteConfirmacaoCentavos --> Negado: C5, E16 confirmacao inconsistente
    PendenteBiometria --> Aprovado: E08 match
    PendenteBiometria --> PendenteBiometria: E08 no-match, 1a vez
    PendenteBiometria --> Negado: E17 falha biometrica recorrente
    Aprovado --> [*]
    Negado --> [*]
    RevisaoManual --> Negado: fraude confirmada
    RevisaoManual --> Aprovado: falso positivo
```

### 4. Diagrama de sequência: combo suspeito até decisão por histórico (cenário C3)

Mostra como dois eventos simples do mesmo pedido são correlacionados pelo motor de CEP em [E13](#e13), e como a decisão final consulta o histórico do cliente antes de agir, evitando negar um cliente recorrente.

```mermaid
sequenceDiagram
    participant Cliente
    participant Checkout as Checkout Service
    participant Kafka
    participant CEP as Motor de CEP
    participant Decisao as Serviço de decisão
    participant Historico as Histórico do cliente

    Cliente->>Checkout: adiciona item de categoria de risco
    Checkout->>Kafka: publica E01
    Cliente->>Checkout: valor total sobe do limite
    Checkout->>Kafka: publica E02
    Kafka->>CEP: consome E01 e E02, mesmo orderId
    CEP->>CEP: avalia conjunção (valor alto AND categoria de risco)
    CEP->>Kafka: publica E13, compra suspeita
    Kafka->>Decisao: consome E13
    Decisao->>Historico: consulta padrão de compra do cliente
    Historico-->>Decisao: histórico compatível ou não
    alt histórico compatível, cliente recorrente
        Decisao->>Cliente: aprova pedido
    else histórico incompatível, cliente novo com esse padrão
        Decisao->>Cliente: nega pedido
    end
```

### 5. Diagrama de sequência: correlação multi-sinal até scan facial (cenário C2)

Mostra três eventos simples de origens diferentes (dispositivo, endereço, valor) sendo correlacionados numa janela deslizante, resultando em [E14](#e14) e na exigência de biometria antes de aprovar o pedido.

```mermaid
sequenceDiagram
    participant Device as Device Fingerprinting
    participant Cliente
    participant Checkout as Checkout Service
    participant Kafka
    participant CEP as Motor de CEP
    participant Biometria as Serviço de biometria

    Device->>Kafka: publica E09, novo dispositivo
    Cliente->>Checkout: informa endereço diferente do histórico
    Checkout->>Kafka: publica E03
    Cliente->>Checkout: valor total atualizado, alto
    Checkout->>Kafka: publica E02
    Kafka->>CEP: consome E09, E03, E02, mesmo clienteId, janela de 10 min
    CEP->>CEP: avalia conjunção com janela deslizante
    CEP->>Kafka: publica E14, possível conta comprometida
    Kafka->>Checkout: consome E14
    Checkout->>Cliente: solicita scan facial, E07
    Cliente->>Biometria: realiza scan
    Biometria->>Kafka: publica E08, resultado
    alt match
        Checkout->>Cliente: aprova pedido
    else no-match
        Checkout->>Cliente: pede novo scan, ou nega se virar E17
    end
```

### 6. Diagrama de sequência: padrão de teste de cartão até bloqueio (cenário C4)

Mostra uma sequência de tentativas de pagamento pequenas e rápidas sendo contadas pelo motor de CEP até ultrapassar o limite, gerando [E15](#e15) e o bloqueio preventivo da conta.

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
        Checkout->>Kafka: publica E04
    end
    Kafka->>CEP: consome sequência de tentativas, mesmo clienteId/dispositivo
    CEP->>CEP: avalia contagem maior ou igual a N, janela curta
    CEP->>Kafka: publica E15, padrão de teste de cartão
    Kafka->>Decisao: consome E15
    Decisao->>Checkout: bloqueia conta
```
