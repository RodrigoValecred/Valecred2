# Regras de Inteligência de Agentes de IA para a Camada Gold

**1. Oficializar a Estratégia de "Tabela Fato Única" (Resgate do Histórico)**
Títulos deferidos e indeferidos não devem ser fragmentados em tabelas separadas, mas consolidados em uma única Fato controlada pela coluna `status_deferimento`. Isso evita duplicidade de relações no modelo em estrela e aproveita ao máximo a performance de compressão colunar no modo Import do Fabric.

**2. Integração explícita com a LGPD e o Controle de RLS**
A Camada Gold só pode consumir as Surrogate Keys, sendo expressamente proibido o trânsito de chaves primárias originais (como o CPF real) em seus modelos de entrega. Além disso, é na Camada Gold que a Segurança Nativa de Nível de Linha (RLS) será ativada e herdada pelos relatórios, garantindo conformidade com a auditoria.

**3. Regulamentação do Modo de Conexão (Import vs. DirectQuery)**
A Gold entregará tabelas em modo Import para painéis táticos de Diretoria e históricos massivos de 5 anos. Já as tabelas preparadas para DirectQuery (como a `TV_KPI_VOP_HOJE` para a Mesa de Operações) visam atualizações a cada 6 minutos, equilibrando custo e velocidade na arquitetura híbrida.

**4. Fragmentação da Gold em "Data Marts" Departamentais**
Criação de Lakehouses Gold departamentais (ex: Gold_Risco, Gold_Comercial, Gold_Financeiro), que funcionam como Data Marts. Isso atende à regra de Ambientes Segregados e Hierarquia de Acessos, garantindo que a Diretoria ou analistas específicos acessem apenas os "pacotes" da Gold que lhes competem, sem misturar os modelos lógicos na Arquitetura Mesh.

**5. Flexibilização das Regras para a Inteligência Artificial (V.A.I.)**
Criação de uma Feature Store (Loja de Variáveis) isolada dentro da Gold. Isso permite que relatórios continuem consumindo texto legível e que apenas a Feature Store sirva matrizes matemáticas exclusivas para o aprendizado de máquina, permitindo que os algoritmos modernos lidem com categorias em texto e valores nulos sem a necessidade de "matematizar" todas as tabelas.

**6. Prevenção Absoluta de Ambiguidade (Bugs de Joins)**
Nós não quebramos dashboards de diretoria por erros de esquema. Sempre que cruzar tabelas que possuem a mesma chave (ex: `cod_cliente` existe tanto em `carteira_de_titulos` quanto em `fato_operacoes`), o agente deve usar `.drop('cod_cliente')` na tabela secundária *antes* do join, garantindo que o Power BI nunca fique confuso sobre de onde vem o dado.

**7. Cristalização de KPIs e Métricas Oficiais (A Única Fonte da Verdade)**
Métricas de negócio não podem ser calculadas de duas formas diferentes. O cálculo do Volume Operado (VOP) deve agrupar por `data_deferimento` e somar o `valor_de_face`, filtrando operações aprovadas (`status_analise == 'D'`). Transações de controle sistêmico (como os tipos 'PR', 'RC', 'RE' ou linhas de produto como 'AB', 'AM', 'LB') devem ser ativamente filtradas da `fato_operacoes` principal para não inflarem falsamente o faturamento no dashboard da TV.

**8. Performance Extrema de Agregação (Proteção do Cluster)**
Nós entregamos consultas em milissegundos sem estourar custos. É estritamente proibido rodar vários `.count()` ou `.collect()` em sequência para tirar totais, pois isso força o banco a varrer a tabela inteira múltiplas vezes. Agrupe as matemáticas em uma única operação `.agg()` ou `.select()`. Se uma tabela intermediária precisar ser salva em memória (`.cache()`), a rotina *deve* envolver o uso em um bloco `try...finally` chamando `.unpersist()`. Isso previne vazamentos de memória (Memory Leaks) que derrubam o cluster.