# Análise Técnica: Clusterização de Clientes para Risco e Comportamento

Este documento avalia a proposta original de segmentação de clientes baseada em Taxas de Juros e detalha a implementação adotada, fundamentada em melhores práticas de mercado para análise de risco de crédito (Credit Risk Modeling) e comportamento do consumidor (Behavioral Scoring).

## 1. Avaliação da Proposta Original
**Ideia Inicial:** Clusterizar clientes utilizando `Taxa_Efetiva_Operacoes` e `Taxa_Final`.

### Crítica Técnica
A utilização exclusiva de taxas de juros para segmentar risco apresenta limitações conceituais importantes:
1.  **Variável de Precificação vs. Comportamento:** A taxa é uma variável definida pela empresa (exógena ao comportamento intrínseco do cliente no momento do pagamento). Um cliente pode ter uma taxa alta por ser novo (risco desconhecido) e pagar pontualmente (comportamento "Prime"), ou ter uma taxa baixa por relacionamento antigo e começar a atrasar (comportamento "Alerta").
2.  **Causalidade Inversa:** A taxa deveria refletir o risco, e não definí-lo. Agrupar por taxas informa "como precificamos", mas não "como o cliente paga".
3.  **Incapacidade de Prever Degradação:** Taxas fixas contratadas não capturam a piora recente (ex: cliente que sempre pagou em dia e começou a atrasar nos últimos 3 meses).

## 2. Abordagem Implementada (Melhores Práticas de Mercado)
Para responder às perguntas de negócio ("quem paga em dia", "quem atrasa mas paga", "quem está piorando"), adotamos uma modelagem comportamental baseada em **RFM (Recência, Frequência, Valor)** e **Vintage Analysis**.

### Variáveis Selecionadas
| Variável | Conceito de Mercado | O que responde? |
| :--- | :--- | :--- |
| **Média de Atraso Histórico** | *Latency / Delinquency* | O cliente paga em dia ou costuma atrasar? |
| **Taxa de Pontualidade** | *Frequency / Reliability* | Qual a probabilidade de um título ser pago sem atraso? |
| **Tendência de Atraso** | *Momentum / Degradation* | O comportamento recente (90d) está pior que o histórico (180d)? Captura a "piora de cenário". |
| **Saldo Inadimplente Atual** | *Exposure at Default (EAD)* | Qual o risco financeiro imediato em aberto? |
| **Desvio Padrão do Atraso** | *Volatility / Stability* | O cliente é consistente ou imprevisível? (Adicionado na V2) |
| **Volume Total Pago** | *Monetary Value* | Qual a relevância financeira do cliente? (Adicionado na V2) |

### Algoritmo: K-Means
- **Normalização (StandardScaler):** Essencial, pois misturamos dias (0-30), taxas (0-1) e valores monetários (milhões).
- **Número de Clusters (K=3):** Definido por necessidade de negócio ("Semáforo": Verde/Amarelo/Vermelho), validado via Silhouette Score.

## 3. Interpretação dos Perfis (Labels Dinâmicos)
A clusterização não supervisionada pode gerar IDs aleatórios. Implementamos uma lógica de **Labeling Dinâmico** baseada nos centroides:

1.  **Cluster A (Prime/Estável):** Menor média de atraso, alta pontualidade. Baixa volatilidade.
2.  **Cluster B (Rentável/Moderado):** Atraso médio positivo (gera receita de juros), mas sem tendência de piora grave ou inadimplência crítica.
3.  **Cluster C (Alerta/Risco):** Maior atraso, tendência de piora positiva (recente > antigo) e saldo em aberto relevante.

## 4. Próximos Passos (Recomendados)
1.  **Monitoramento de Safra (Vintage):** Acompanhar a migração de clientes entre clusters ao longo do tempo (Matriz de Migração).
2.  **Ação Recomendada:**
    - *Prime:* Aumentar limite, oferecer taxas melhores para fidelização.
    - *Rentável:* Manter monitoramento, oferecer produtos de antecipação.
    - *Alerta:* Bloqueio de novos limites, ação de cobrança preventiva.
