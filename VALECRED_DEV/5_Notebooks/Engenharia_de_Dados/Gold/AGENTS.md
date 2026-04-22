# Regras de Inteligência de Agentes de IA para a Camada Gold

**1. Modelagem Dimensional Rigorosa (Star Schema Puro)**
A Gold não é um espelho da Silver. Aqui entregamos modelos otimizados para dashboards.
•	**Fatos e Dimensões:** Separe rigorosamente eventos transacionais (ex: `fato_operacoes`, `fato_titulos`) de entidades descritivas (ex: `dim_clientes`).
•	**Cruzamentos Inteligentes:** Ao juntar tabelas, use ativamente `broadcast()` nas dimensões pequenas para evitar gargalos de rede, mas *NUNCA* faça broadcast de tabelas fato massivas, deixando o Spark AQE (Adaptive Query Execution) gerenciar os dados pesados.

**2. Prevenção Absoluta de Ambiguidade (Bugs de Joins)**
Nós não quebramos dashboards de diretoria por erros de esquema.
•	**Limpeza de Chaves (AMBIGUOUS_REFERENCE):** Sempre que cruzar tabelas que possuem a mesma chave (ex: `cod_cliente` existe tanto em `carteira_de_titulos` quanto em `fato_operacoes`), o agente deve usar `.drop('cod_cliente')` na tabela secundária *antes* do join, garantindo que o Power BI nunca fique confuso sobre de onde vem o dado.

**3. Cristalização de KPIs e Métricas Oficiais (A Única Fonte da Verdade)**
Métricas de negócio não podem ser calculadas de duas formas diferentes.
•	**Cálculo do Volume Operado (VOP):** Sempre que calcular VOP, o padrão é agrupar por `data_deferimento` e somar o `valor_de_face`, filtrando operações aprovadas (`status_analise == 'D'`).
•	**Filtro de Ruído Operacional:** Transações de controle sistêmico (como os tipos 'PR', 'RC', 'RE' ou linhas de produto como 'AB', 'AM', 'LB') devem ser ativamente filtradas da `fato_operacoes` principal para não inflarem falsamente o faturamento no dashboard da TV.

**4. Performance Extrema de Agregação (Proteção do Cluster)**
Nós entregamos consultas em milissegundos sem estourar custos.
•	**DAGs Otimizadas:** É estritamente proibido rodar vários `.count()` ou `.collect()` em sequência para tirar totais, pois isso força o banco a varrer a tabela inteira múltiplas vezes. Agrupe as matemáticas em uma única operação `.agg()` ou `.select()`.
•	**Segurança de Memória:** Se uma tabela intermediária precisar ser salva em memória (`.cache()`), a rotina *deve* envolver o uso em um bloco `try...finally` chamando `.unpersist()`. Isso previne vazamentos de memória (Memory Leaks) que derrubam o cluster.

**5. Preparação Ativa para a Inteligência Artificial (V.A.I.)**
A Gold alimenta o cérebro do banco. Os dados devem estar prontos para a matemática.
•	As tabelas agregadas que servem a IA (como o `Perfil_Analitico_Sacado`) devem conter cálculos vetoriais puros. Por exemplo, desvios e limites (como Z-scores para análise de risco da V.A.I.) exigem dados numéricos contínuos sem nulos, permitindo que a IA aplique regras como a detecção de *Notas Sequenciais (Risco de Fuga)* com base nas datas exatas de entrada e volume financeiro.