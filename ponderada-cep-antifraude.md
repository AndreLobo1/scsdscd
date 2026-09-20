# Ponderada: Complex Event Processing (CEP)

## Software escolhido

Antifraude do checkout do site shopper.com.br

### Contexto

Shopper é um mercado online: cliente compra pelo site ou app, sem contato presencial no momento da compra. Por não existir verificação humana no caixa, o antifraude é o sistema que decide, durante o checkout, se o pedido segue direto, passa por verificação extra, ou é barrado, cruzando sinais do pedido (itens, valor, dispositivo, endereço) pra separar cliente legítimo de fraude sem travar a operação com falso positivo.

NSU (identificador de transação de adquirente/maquininha) não se aplica aqui: é campo de pagamento presencial via adquirente, e o domínio escolhido é checkout de e-commerce sem esse componente. Fora de escopo por natureza do domínio, não por omissão.

### Conceito de CEP que entendi com base nos autoestudos

Entendimento: eventos simples A e B acontecem, o motor de CEP os processa em tempo real e, ao encontrar um padrão entre eles, gera um evento complexo C, derivado dos dois primeiros. Isso dá ao sistema o poder de reagir à fraude "ao vivo", durante o checkout, em vez de descobrir o problema depois, numa análise em lote no fim do dia.

Aplicando ao meu exemplo: [E01](#e01) (item de risco) e [E02](#e02) (valor atualizado) são o A e o B. O motor de CEP aplica uma regra de conjunção sobre os dois, escopados ao mesmo pedido, enquanto o pedido está em `Criado`/`EmAnalise`, e gera [E13](#e13) (compra suspeita), que é o C. A reação (pedir verificação extra ou negar) acontece enquanto o pedido ainda está aberto, porque o processamento é sobre o stream, não sobre um relatório do dia seguinte.

#### Critério de classificação usado na tabela de eventos

| Classificação | Definição | Teste de inclusão |
|---|---|---|
| Simples | Fato atômico, observado direto na fonte | Existe sozinho, sem precisar correlacionar com nenhum outro evento |
| Complexo | Evento derivado | Só existe como resultado de aplicar um operador de correlação (conjunção, janela deslizante, contagem/sequência, ausência) sobre 2 ou mais eventos simples, ou sobre a falta de um deles |
| Negócio | Representa um fato do processo de compra/pagamento | Relevante pro domínio em si, independente de qual tecnologia processa ele |
| Técnico | Se origina de um componente de infraestrutura/plataforma (fingerprinting, gateway, broker) | Só entra na modelagem se alimentar uma decisão do motor de fraude, virando input de um evento complexo ou mudando uma ação do sistema. Evento técnico que não afeta nenhuma decisão de negócio (ex: métrica genérica de saúde do broker) fica fora do escopo, é operação da plataforma Kafka, não evento do domínio de antifraude |

## 1. Tabela de eventos

Os 21 eventos abaixo foram levantados a partir do fluxo de checkout descrito no contexto e classificados segundo o critério acima. Todo evento carrega `orderId` e `sessionId` (ver diagrama 1), as correlações usam a chave que faz sentido pro caso: `orderId` pra eventos do mesmo pedido, `sessionId` pra eventos que podem repetir antes do pedido fechar (confirmação, biometria), `clienteId` pra correlação entre pedidos do mesmo cliente, e o valor de fingerprint/endereço em si pra correlação entre clientes diferentes.

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
| <a id="e10"></a>E10 | Timeout ou falha na integração com gateway de pagamento | Erro de rede/infra na chamada ao gateway | Simples | Técnico | Usado pelo processador de [E15](#e15) pra excluir a tentativa do contador de fraude: falha do gateway não é a mesma coisa que fraudador testando cartão, sem essa exclusão o contador gera falso positivo |
| <a id="e11"></a>E11 | Falha na chamada ao serviço de biometria | Serviço de scan facial fica indisponível ou retorna erro | Simples | Técnico | Usado pelo processador de [E17](#e17) pra não contar indisponibilidade do serviço como reprovação (no-match): sem essa exclusão, uma falha de infra viraria bloqueio de cliente legítimo |
| <a id="e12"></a>E12 | Latência alta na chamada ao serviço de device fingerprinting | [E09](#e09) demora além do aceitável pra chegar | Simples | Técnico | Quando ultrapassa a janela de correlação de [E14](#e14), dispara [E19](#e19), formalizando o padrão de ausência de evento em vez de deixar o motor esperando indefinidamente |
| <a id="e13"></a>E13 | Compra suspeita | Valor do pedido acima do limite e item de categoria de risco no mesmo pedido, enquanto o pedido está em `Criado`/`EmAnalise` | Complexo | Negócio | Conjunção (AND) de [E01](#e01) e [E02](#e02) escopada ao mesmo `orderId`. O que faz isso ser correlação de CEP e não um `if` disfarçado: [E01](#e01) e [E02](#e02) são publicados por partes distintas do checkout e não chegam em ordem garantida, o motor mantém um "partial match" (retém o primeiro evento até o par completar), igual pattern matching de motores CEP tipo Esper. Esse partial match tem TTL igual ao timeout de carrinho abandonado da loja: se o pedido nunca sai de `Criado` dentro desse prazo, o estado retido expira e é descartado, evitando acúmulo indefinido no state store |
| <a id="e14"></a>E14 | Possível conta comprometida | [E09](#e09), [E03](#e03) e valor alto ([E02](#e02)) dentro de uma janela curta (ex: 10 min), mesmo `clienteId` | Complexo | Negócio | Conjunção com janela deslizante sobre 3 eventos de origens distintas, a coincidência na janela é o que importa, não a ordem. [E03](#e03) e [E02](#e02) nascem chaveados por `orderId`, precisam de repartição pra `clienteId` antes do join (detalhe no diagrama 2), já que o mesmo cliente pode ter feito o pedido em sessões diferentes |
| <a id="e15"></a>E15 | Padrão de teste de cartão | Múltiplas tentativas de [E04](#e04) falhando dentro do mesmo `orderId`, antes do pedido fechar, líquido de exclusões de [E10](#e10) | Complexo | Negócio | Contagem com threshold sobre repetição do mesmo tipo de evento, escopada ao mesmo pedido (mesma chave de [E04](#e04)/[E10](#e10), sem repartição necessária, ao contrário de [E14](#e14)) |
| <a id="e16"></a>E16 | Confirmação inconsistente | 2 ou mais ocorrências de [E06](#e06) com erro, dentro do mesmo `sessionId` | Complexo | Negócio | Contagem com escopo de sessão (`sessionId`, atributo de `EventoAntifraude`, ver diagrama 1), evento simples repetido vira gatilho só ao ultrapassar o threshold |
| <a id="e17"></a>E17 | Falha biométrica recorrente | 2 ou mais ocorrências de [E08](#e08) com no-match, dentro do mesmo `sessionId`, líquido de exclusões de [E11](#e11) | Complexo | Negócio | Mesmo mecanismo de [E16](#e16), aplicado a outro evento simples de origem |
| <a id="e18"></a>E18 | Dispositivo ou endereço compartilhado entre múltiplas contas | Mesmo fingerprint ou endereço aparece em pedidos de `clienteId` diferentes numa janela curta | Complexo | Negócio | Correlação cross-entity: a chave de correlação é o valor do fingerprint/endereço em si, não `orderId` nem `clienteId`, por isso precisa de um índice compartilhado à parte, repartido por esse valor (detalhe no diagrama 2), não cabe em partição por pedido/cliente |
| <a id="e19"></a>E19 | Decisão tomada sem sinal de dispositivo | Janela de correlação de [E14](#e14) expira sem [E09](#e09) chegar (efeito de [E12](#e12)) | Complexo | Negócio | Padrão de ausência: o evento complexo é gerado pela falta de um sinal dentro da janela, não pela presença dele. Precisa de um agendador de expiração de janela (ver `AgendadorDeJanela` no diagrama 1), não só de um operador que reage a chegada de evento |
| <a id="e20"></a>E20 | Escalada ordenada de risco | [E03](#e03), seguido de [E09](#e09), seguido de [E02](#e02) alto, **nessa ordem exata**, dentro de 15 min | Complexo | Negócio | Padrão de sequência real (A→B→C, tipos diferentes, ordem importa), diferente de [E14](#e14) que é conjunção sem ordem: essa ordem específica (endereço muda antes do dispositivo trocar antes do valor subir) reflete o playbook típico de um ataque de conta tomada em andamento, e não dispara se os mesmos 3 eventos chegarem fora dessa ordem |
| <a id="e21"></a>E21 | Circuit breaker do gateway acionado | N timeouts consecutivos de [E10](#e10) no mesmo gateway, sem relação com nenhum pedido específico | Complexo | Técnico | Contagem sobre evento técnico, decisão puramente de infraestrutura (abrir o circuito, parar de chamar o gateway por X segundos), não é fato de negócio, por isso não dispara nenhuma `AcaoAntifraude` da hierarquia de negócio, é tratado à parte pela camada de infra |

## 2. Modelagem estática e dinâmica (UML)

Seis diagramas cobrem os fluxos: dois estáticos (estrutura e arquitetura) e quatro dinâmicos (ciclo de vida do pedido e três sequências, uma por mecanismo de correlação distinto da tabela de eventos).

### 2.1 Modelagem estática

#### Diagrama 1: diagrama de classes, estrutura de domínio

Modela as entidades do pedido e a hierarquia de eventos. `EventoComplexo` tem duas formas: a comum, que agrega os eventos simples presentes que correlacionou (`eventosOrigem`, podendo ser vazio), e `EventoDeAusencia` (caso de [E19](#e19)), que nasce da falta de um evento esperado dentro de uma `JanelaDeCorrelacao`, monitorada por um `AgendadorDeJanela` que dispara a avaliação quando a janela expira sem o sinal chegar. `RegraDeCorrelacao` não carrega mais `janela: Duration` genérico na interface, porque nem toda implementação usa duração (`RegraConjuncao`, de [E13](#e13), é escopada por estado do pedido, não por tempo), cada implementação concreta declara o atributo que faz sentido pra ela.

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
        +status: StatusCliente
        +enderecosHistorico: string[]
        +dispositivosHistorico: string[]
    }
    class StatusCliente {
        <<enumeration>>
        ATIVA
        BLOQUEADA
    }
    class EventoAntifraude {
        <<abstract>>
        +eventoId: string
        +timestamp: datetime
        +orderId: string
        +sessionId: string
    }
    class EventoSimples
    class EventoComplexo {
        +eventosOrigem: EventoSimples[]
    }
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
    class RegraSequencia {
        +janela: Duration
        +ordem: string[]
    }
    class RegraContagem {
        +janela: Duration
        +threshold: int
    }
    class RegraCrossEntity {
        +janela: Duration
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
    class SolicitarNovaConfirmacao

    Pedido "1" *-- "1..*" ItemPedido : composição
    ItemPedido --> Categoria
    Pedido --> Cliente
    Cliente --> StatusCliente
    EventoAntifraude <|-- EventoSimples
    EventoAntifraude <|-- EventoComplexo
    EventoComplexo <|-- EventoDeAusencia
    EventoDeAusencia --> JanelaDeCorrelacao
    AgendadorDeJanela --> JanelaDeCorrelacao : monitora
    RegraAusencia --> AgendadorDeJanela : assina expiração
    EventoComplexo --> RegraDeCorrelacao
    EventoComplexo "1" o-- "0..*" EventoSimples : agregação, correlaciona
    EventoComplexo --> AcaoAntifraude : dispara
    RegraDeCorrelacao <|.. RegraConjuncao
    RegraDeCorrelacao <|.. RegraJanelaDeslizante
    RegraDeCorrelacao <|.. RegraSequencia
    RegraDeCorrelacao <|.. RegraContagem
    RegraDeCorrelacao <|.. RegraCrossEntity
    RegraDeCorrelacao <|.. RegraAusencia
    AcaoAntifraude <|.. CobrancaCentavos
    AcaoAntifraude <|.. ScanFacial
    AcaoAntifraude <|.. NegarPedido
    AcaoAntifraude <|.. BloquearConta
    AcaoAntifraude <|.. SolicitarNovaConfirmacao
```

`RegraConjuncao` implementa [E13](#e13), `RegraJanelaDeslizante` implementa [E14](#e14), `RegraSequencia` implementa [E20](#e20) (única regra em que a ordem de chegada dos tipos de evento importa, diferente de todas as outras), `RegraContagem` implementa [E15](#e15)/[E16](#e16)/[E17](#e17)/[E21](#e21) (uma instância por evento, mesmo mecanismo, [E21](#e21) é a única de tipo técnico), `RegraCrossEntity` implementa [E18](#e18), `RegraAusencia` implementa [E19](#e19) e é a única que produz um `EventoDeAusencia` em vez de um `EventoComplexo` comum.

#### Diagrama 2: diagrama de arquitetura de streaming

Este é um diagrama de arquitetura/fluxo (não um diagrama de componentes UML formal, sem estereótipo `<<component>>` nem notação de interface lollipop/socket), a escolha aqui é mostrar o roteamento real de tópico por evento, que é o que a tabela 1 precisa provar que sustenta.

Cada serviço de origem é um producer que publica num tópico Kafka próprio por tipo de evento, com a chave de partição natural indicada em cada tópico. Duas correlações não batem com a chave natural do tópico de origem, e precisam de repartição (`selectKey`) explícita antes do processador de correlação:

- [E14](#e14) correlaciona por `clienteId`, mas `cart-value-updated` e `address-check` nascem chaveados por `orderId` (um cliente pode ter feito o pedido em sessões diferentes), então passam por `RK1` antes de chegar no processador.
- [E18](#e18) correlaciona pelo valor do fingerprint/endereço em si, então passa por `RK2` até virar a `GlobalKTable` compartilhada.

[E15](#e15), ao contrário do que uma versão anterior deste documento afirmava, foi reescopado pra correlacionar dentro do **mesmo `orderId`** (múltiplas tentativas de pagamento no mesmo pedido antes dele fechar), que já é a chave natural de `payment-attempt`/`gateway-failure`, então não precisa de repartição nenhuma.

```mermaid
flowchart LR
    subgraph Producers["Producers"]
        A1[Checkout Service]
        A2[Payment Gateway]
        A3[Device Fingerprinting]
        A4[Biometria Service]
    end

    subgraph Topicos["Kafka: chave de partição natural indicada em cada tópico"]
        T1[(cart-item-added, E01, chave=orderId)]
        T2[(cart-value-updated, E02, chave=orderId)]
        T3[(address-check, E03, chave=orderId)]
        T4[(payment-attempt, E04, chave=orderId)]
        T5[(payment-confirmation, E06, chave=sessionId)]
        T6[(device-fingerprint, E09, chave=clienteId)]
        T7[(biometric-result, E08, chave=sessionId)]
        T8[(gateway-failure, E10, chave=orderId)]
        T9[(biometria-failure, E11, chave=sessionId)]
        T10[(fingerprint-latency, E12, chave=clienteId)]
    end

    RK1[Repartição: selectKey por clienteId]
    RK2[Repartição: selectKey por fingerprint/endereço]
    IDXTOPIC[(identity-index-topic, compactado, chave=fingerprint/endereço)]
    IDX[("identity-index, GlobalKTable")]

    subgraph CEPSingle["Motor de CEP: correlação single-entity"]
        P13[RegraConjuncao -> E13]
        P14[RegraJanelaDeslizante -> E14]
        P20[RegraSequencia -> E20]
        P19[RegraAusencia -> E19]
        P15[RegraContagem -> E15]
        P16[RegraContagem -> E16]
        P17[RegraContagem -> E17]
        P21[RegraContagem -> E21, técnico]
    end

    subgraph CEPCross["Motor de CEP: correlação cross-entity"]
        P18[RegraCrossEntity -> E18]
    end

    S1[(fraud-complex-events)]

    subgraph Consumers["Consumers de decisão"]
        D1[Serviço de decisão]
        D2[Notificação]
        D3[Time de risco]
        D4[Camada de infra: circuit breaker]
    end

    A1 --> T1
    A1 --> T2
    A1 --> T3
    A1 --> T4
    A2 --> T5
    A2 --> T8
    A3 --> T6
    A3 --> T10
    A4 --> T7
    A4 --> T9

    T1 --> P13
    T2 --> P13
    T6 --> P14
    T3 --> RK1
    T2 --> RK1
    RK1 --> P14
    T3 --> P20
    T6 --> P20
    T2 --> P20
    T10 --> P19
    T4 --> P15
    T8 --> P15
    T5 --> P16
    T7 --> P17
    T9 --> P17
    T8 --> P21

    T6 --> RK2
    T3 --> RK2
    RK2 --> IDXTOPIC
    IDXTOPIC --> IDX
    IDX --> P18

    P13 --> S1
    P14 --> S1
    P20 --> S1
    P19 --> S1
    P15 --> S1
    P16 --> S1
    P17 --> S1
    P18 --> S1

    S1 --> D1
    S1 --> D2
    S1 --> D3
    P21 --> D4
```

### 2.2 Modelagem dinâmica

#### Diagrama 3: diagrama de estados, ciclo de vida do pedido

Representa como o pedido transita entre estados. Todos os rótulos de transição abaixo são eventos ou triggers nomeados, não números de cenário (o mapeamento pra cenário de negócio fica só no texto, não no diagrama, pra não misturar os dois vocabulários): `AvaliacaoConcluidaSemComplexo` corresponde ao caminho de aprovação direta, `SinalIsoladoDeRisco` corresponde ao cenário [C1](#c1) (nenhuma `RegraDeCorrelacao` disparou, só um sinal fraco isolado), `E13` leva a `AvaliandoHistorico` e corresponde ao [C3](#c3) (diagrama 4 detalha essa consulta), `E14`/`E19` correspondem ao [C2](#c2), `E15` ao [C4](#c4), `E16`/`E17` fecham [C5](#c5)/[C2](#c2). [E18](#e18) e [E20](#e20) também aparecem (indo pra `RevisaoManual`), eventos reais da tabela 1 com reação de sistema definida, mesmo sem virar cenário numerado da seção 3. Repetições que não mudam de estado (1º erro de confirmação, 1ª reprovação de biometria) ficam de fora do diagrama, só saem do estado pendente quando o evento complexo correspondente é gerado.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Criado
    Criado --> EmAnalise : E04
    EmAnalise --> Aprovado : AvaliacaoConcluidaSemComplexo
    EmAnalise --> PendenteCentavos : SinalIsoladoDeRisco
    EmAnalise --> AvaliandoHistorico : E13
    EmAnalise --> PendenteBiometria : E14
    EmAnalise --> PendenteBiometria : E19
    EmAnalise --> Negado : E15
    EmAnalise --> RevisaoManual : E18
    EmAnalise --> RevisaoManual : E20
    AvaliandoHistorico --> Aprovado : HistoricoCompativel
    AvaliandoHistorico --> Negado : HistoricoIncompativel
    PendenteCentavos --> Aprovado : E06Correto
    PendenteCentavos --> Negado : E16
    PendenteBiometria --> Aprovado : E08Match
    PendenteBiometria --> Negado : E17
    RevisaoManual --> Negado : FraudeConfirmada
    RevisaoManual --> Aprovado : FalsoPositivo
    Aprovado --> [*]
    Negado --> [*]
```

#### Diagrama 4: diagrama de sequência, combo suspeito até decisão por histórico (cenário C3)

Mostra como dois eventos simples do mesmo pedido são correlacionados pelo motor de CEP em [E13](#e13) via partial match (o processador retém o primeiro evento que chegar até o par completar), e como a decisão final consulta o histórico do cliente antes de agir, evitando negar um cliente recorrente. O histórico de compra consultado aqui vive no sistema de gestão de pedidos, fora da arquitetura de streaming do antifraude, é uma consulta síncrona a outro bounded context, não parte do pipeline de CQRS descrito na seção 2.3.

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
    CEP->>Kafka: publica E13, compra suspeita
    Kafka->>Decisao: consome E13
    Decisao->>Historico: consulta síncrona ao padrão de compra do cliente
    Historico-->>Decisao: histórico compatível ou não
    alt histórico compatível, cliente recorrente
        Decisao->>Cliente: aprova pedido
    else histórico incompatível, cliente novo com esse padrão
        Decisao->>Cliente: nega pedido
    end
```

#### Diagrama 5: diagrama de sequência, correlação multi-sinal, ausência de sinal e scan facial (cenário C2)

Mostra três eventos simples de origens diferentes (dispositivo, endereço, valor) sendo correlacionados numa janela deslizante, resultando em [E14](#e14). Inclui o caminho alternativo em que o dispositivo não responde a tempo: o `AgendadorDeJanela` detecta a expiração, [E12](#e12) é publicado, e o motor gera [E19](#e19) (ausência de sinal) em vez de travar esperando, seguindo pro mesmo fallback conservador.

```mermaid
sequenceDiagram
    participant Device as Device Fingerprinting
    participant Cliente
    participant Checkout as Checkout Service
    participant Kafka
    participant CEP as Motor de CEP
    participant Agendador as Agendador de janela
    participant Biometria as Serviço de biometria

    Cliente->>Checkout: informa endereço diferente do histórico
    Checkout->>Kafka: publica E03
    Cliente->>Checkout: valor total atualizado, alto
    Checkout->>Kafka: publica E02
    Kafka->>CEP: consome E03 e E02, mesmo clienteId
    CEP->>Agendador: abre janela de 10 min aguardando E09
    alt E09 chega dentro da janela
        Device->>Kafka: publica E09, novo dispositivo
        Kafka->>CEP: consome E09
        CEP->>CEP: avalia conjunção com janela deslizante
        CEP->>Kafka: publica E14, possível conta comprometida
    else janela expira sem E09
        Device->>Kafka: publica E12, latência alta
        Agendador->>CEP: notifica expiração da janela
        CEP->>Kafka: publica E19, decisão sem sinal de dispositivo
    end
    Kafka->>Checkout: consome E14 ou E19
    Checkout->>Cliente: solicita scan facial, E07
    Cliente->>Biometria: realiza scan
    Biometria->>Kafka: publica E08, resultado
    alt match
        Checkout->>Cliente: aprova pedido
    else no-match
        Checkout->>Cliente: pede novo scan, ou nega se virar E17
    end
```

#### Diagrama 6: diagrama de sequência, padrão de teste de cartão até bloqueio (cenário C4)

Mostra uma sequência de tentativas de pagamento pequenas e rápidas sendo contadas pelo motor de CEP até ultrapassar o limite, gerando [E15](#e15) e o bloqueio preventivo da conta. Uma falha de gateway ([E10](#e10)) no meio da sequência é descontada da contagem. Cada tentativa carrega um `eventoId` idempotente: se o producer reenviar a mesma tentativa (garantia at-least-once do Kafka), o processador de contagem reconhece o `eventoId` repetido e não conta duas vezes.

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
    Gateway--)Kafka: publica E10, falha de gateway, 1 tentativa descontada
    Kafka->>CEP: consome sequência de tentativas, mesmo orderId, dedup por eventoId, líquido de E10
    CEP->>CEP: avalia contagem maior ou igual a N, janela curta
    CEP->>Kafka: publica E15, padrão de teste de cartão
    Kafka->>Decisao: consome E15
    Decisao->>Checkout: bloqueia conta
```

### 2.3 Event Sourcing e CQRS aplicados à arquitetura

**Event Sourcing, com a ressalva certa**: os tópicos de evento (`cart-item-added`, `device-fingerprint`, `fraud-complex-events` etc) são um log append-only, útil pra auditoria (reconstruir "o que aconteceu com esse pedido" replayando o stream daquele `orderId`). Isso não é Event Sourcing pleno enquanto `Pedido.status` (diagrama 1) continua sendo um campo mutável escrito diretamente pelos serviços de decisão: Event Sourcing de verdade exigiria que `status` fosse uma projeção, recalculada aplicando uma função de fold sobre o log de eventos daquele `orderId`, nunca escrita direto. Com a arquitetura atual, o que existe é log de auditoria com potencial de virar Event Sourcing pleno, não é a mesma coisa, e o documento não afirma mais que é.

**CQRS, separando o exemplo bom do frágil**: o `identity-index` (diagrama 2) é CQRS de verdade, um read model (`GlobalKTable`) materializado a partir do próprio stream de escrita (`device-fingerprint`/`address-check` repartidos), consultado pela correlação cross-entity sem tocar o lado de escrita. Já o "histórico de compra do cliente" (diagrama 4) não é exemplo de CQRS desta arquitetura: nada aqui mostra esse histórico sendo alimentado pelo stream de antifraude, ele vive no sistema de gestão de pedidos e é consultado de forma síncrona, como uma integração externa, não como o lado de leitura do mesmo pipeline. Deixei isso marcado no texto do diagrama 4.

### 2.4 Semântica de entrega e janelas

Kafka garante at-least-once por padrão: o mesmo evento pode chegar duplicado ao consumer (retry de producer, rebalance de partição). Os operadores de contagem ([E15](#e15), [E16](#e16), [E17](#e17)) usam o `eventoId` de `EventoAntifraude` como chave de deduplicação, sem isso um reenvio de [E04](#e04) contaria duas vezes no threshold de [E15](#e15) e dispararia bloqueio indevido.

Janelas baseadas em tempo ([E14](#e14), [E19](#e19), [E20](#e20)) usam o timestamp do evento, não o de processamento, e um grace period (terminologia de Kafka Streams, que é o motor assumido nesta arquitetura pela `GlobalKTable`/`selectKey` já usados; watermark é terminologia de Flink, não se aplica aqui): um evento que chega depois do grace period (atraso de rede/fila) é tratado como tarde demais pra aquela janela, mesmo que o dado exista, cai no mesmo caminho de [E19](#e19) (ausência), que é justamente o motivo de [E19](#e19) existir como padrão formal em vez de exceção não tratada.

## 3. Cenários de negócio

#### Critério usado pra definir um cenário

| Critério | O que exige |
|---|---|
| Ancorado num evento real da tabela 1 | O evento de entrada é um ID específico ([E01](#e01) a [E19](#e19)) ou a ausência explícita de um evento complexo, nunca uma situação hipotética solta |
| Ação mapeada numa classe concreta | A ação disparada corresponde a uma das implementações de `AcaoAntifraude` do diagrama 1 (`CobrancaCentavos`, `ScanFacial`, `NegarPedido`, `BloquearConta`, `SolicitarNovaConfirmacao`), não uma descrição vaga nova |
| Ganho sobre a transação, não sobre detecção em geral | O benefício declarado precisa ser sobre a transação em si (menos fricção, decisão mais rápida, bloqueio antes da perda), e não um argumento genérico de "detecta mais fraude" |
| Mecanismo distinto dos outros cenários | Cada cenário usa uma `RegraDeCorrelacao` diferente da tabela 1, pra não virar o mesmo padrão com o evento trocado |

[E18](#e18) e [E19](#e19) têm reação de sistema definida (diagramas 2 e 3), mas não viraram um dos 5 cenários abaixo, a ponderada pede 3 no mínimo e esses 5 já cobrem todos os mecanismos de correlação distintos da tabela 1. [C1](#c1) é um caso à parte dentro do próprio critério de mecanismo: não é implementado por nenhuma `RegraDeCorrelacao`, é justamente o caminho em que nenhuma regra disparou, um sinal fraco isolado que não completou nenhum padrão de correlação, por isso a ação dele é mais leve que a dos outros 4.

| ID | Cenário | Evento(s) de entrada | Ação disparada | Ganho de eficiência |
|---|---|---|---|---|
| <a id="c1"></a>C1 | Cobrança de centavos quando nenhuma regra de correlação dispara | 1 sinal de risco isolado (ex: [E03](#e03), sem outros sinais na janela, nenhum `EventoComplexo` gerado) | [E05](#e05) | Evita negar venda legítima por 1 sinal fraco, resolve a ambiguidade com fricção mínima |
| <a id="c2"></a>C2 | Scan facial só com correlação multi-sinal | [E14](#e14) | [E07](#e07) | Biometria só é exigida quando 3 sinais convergem, reduz atrito no checkout da maioria dos pedidos, que não geram [E14](#e14) |
| <a id="c3"></a>C3 | Negação de combo suspeito com exceção por histórico do próprio cliente | [E13](#e13) correlacionado com histórico de compra do mesmo cliente | Negar pedido, ou liberar se o padrão já é recorrente pra esse cliente | Reduz falso positivo em cliente recorrente (ex: churrasco de família), mantendo o bloqueio pra cliente novo com o mesmo padrão |
| <a id="c4"></a>C4 | Bloqueio preventivo por teste de cartão | [E15](#e15) | Bloquear conta | Detecção em tempo real corta a fraude na 3ª/4ª tentativa, antes do cartão testado ser usado numa compra de valor alto |
| <a id="c5"></a>C5 | Escalonamento progressivo por confirmação inconsistente | [E06](#e06) repetido, virando [E16](#e16) | 1º erro: `SolicitarNovaConfirmacao`. 2º erro ([E16](#e16)): `NegarPedido` (mesma transição determinística do diagrama 3, `PendenteCentavos --> Negado`) | Erro isolado de digitação não penaliza cliente legítimo, só o padrão repetido eleva a ação |
