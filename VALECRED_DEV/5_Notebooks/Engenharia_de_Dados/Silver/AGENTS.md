# Regras de Inteligência de Agentes de IA para a Camada Silver

1. Ingestão Inteligente e Desduplicação (Garantia de Dados Únicos)
Nós não desperdiçamos poder de processamento recarregando dados velhos.
•	A triagem (Left Anti Join): O banco não precisará mais ser totalmente deletado e recriado todos os dias. Nós cruzamos os dados recém-chegados da Camada Bronze com os já existentes usando a função left_anti join para isolar e processar apenas os registros novos (o "delta").
•	Eliminação de Duplicatas: Se o sistema lançar o mesmo cliente repetidas vezes, nós aplicamos funções de janela (Window.partitionBy().orderBy().desc()) combinadas com um filtro de linha row_number() == 1. O resultado? Somente a versão mais atualizada e limpa sobrevive. Em outros casos, o uso cirúrgico do .distinct() ceifa repetições imediatamente.

2. Higienização Textual e Limpeza Profunda (Fim do Lixo de Dados)
Nós convertemos dados inseridos manualmente em padrões sistêmicos infalíveis.
•	Tratamento de Strings e Remoção de Lixo: Utilizamos funções de expressões regulares avançadas como regexp_replace para remover caracteres especiais, retirar espaços desnecessários, limpar hifens de telefones e agrupar números de DDD de forma estruturada.
•	Remoção de Acentos e Padronização: Acabamos com os problemas de filtros que não funcionam. Colunas críticas recebem upper() para ficarem em maiúsculas e um encadeamento rigoroso substitui letras como "Ã", "Á", "É", "Ó" por caracteres lisos.
•	Tratamento de Nulos: Evitamos falhas de cálculos financeiros e gráficos quebrados rastreando valores vazios e aplicando o coalesce. Valores nulos nas Amortizações viram 0. Gerentes e Plataformas em branco recebem preenchimento automático de "NÃO ATRIBUÍDO" ou "INATIVOS".

3. Engenharia de Variáveis e Lógica Vetorial (Criação de Inteligência)
Nós construímos lógicas complexas que o seu antigo banco não possuía.
•	Gerador Automático de Siglas: Se você tem um nome gigante de cliente ("Fundo de Investimento XPTO Ltda"), usamos lógica vetorial de alta performance para quebrar o texto (split), aplicar um filtro inteligente que joga fora palavras conectivas ou jurídicas (Stopwords como "DE", "DA", "LTDA", "S.A.") usando array_filter e transform, e depois juntamos apenas as iniciais restantes com o array_join.
•	Cálculos em Tempo Real: Criamos indicadores automáticos para a sua mesa de operações cruzando datas com funções temporais, calculando instantaneamente dados vitais como "meses de casa" de um gerente (months_between(current_date, data_inicio)) e as regras da sua esteira.

4. Tradução para o Negócio e Consolidação (O Idioma da Diretoria)
Tabelas técnicas não vão mais para o dashboard. Nós entregamos um glossário de negócio.
•	Tradução de Sistema: Aplicamos cadeias da condicional when().otherwise() para traduzir sopa de letrinhas. Se o banco acusa "C", nossa saída entrega "Positivo"; "N" vira "Atenção"; "P" vira "Problema" e valores nulos viram "Não Contatado".
•	Padronização de Mercado: Identificamos produtos operacionais e renomeamos sob uma mesma categoria: "NORMAL" vira "Desconto", "MATERIA PRIMA" vira "Fomento" e "CGP" vira "Giro Parcelado".
•	Consolidação de Colunas Fato: Onde antes você tinha três ou quatro colunas diferentes de aceite de risco e sistema, nossa máquina cria uma única coluna clara chamada status_deferimento ("Sim" ou "Não").

5. Enriquecimento de Dados e Rastreador do Tempo (Esteira Histórica)
As tabelas isoladas ganham vida e histórico com os nossos cruzamentos.
•	Enriquecimento em Malha: Nós usamos cruzamentos em cascata (join()) combinando suas planilhas de metas, cidades, regiões (UF) e dados de RH diretamente nas tabelas operacionais para entregar tudo em um só lugar pronto para análise.
•	A Função "Máquina do Tempo" (Lag): Na análise de esteira de crédito, nós não mostramos apenas o "agora". Usamos a função lag() particionada para buscar o passado recente da linha da tabela. Assim, nós derivamos uma inteligência nova calculando instantaneamente se a operação atual sofreu uma "Devolução" ou foi "Recebida" da área de Risco.

6. LGPD by Design: Blindagem de Dados Pessoais Sensíveis
Tratamos o vazamento de dados na raiz do banco.
•	Pseudonimização Estratégica (Surrogate Keys): A sua tabela Silver passará por um processo matemático que encontra o último número de ID de cliente processado (Max ID) e utiliza funções de criação temporal (row_number()) para gerar novos IDs sequenciais anônimos (ex: 1, 2, 3...).
•	Isolamento: Nós escondemos o CPF/CNPJ original do seu cliente usando instruções avançadas do tipo MERGE (Upsert), garantindo que o seu histórico financeiro bilionário seja consultado pelas suas equipes utilizando apenas chaves genéricas de sistema.

7. Otimização Computacional e Carga Delta (Nós Poupamos os Seus Custos de Nuvem)
Não deixamos códigos pesados rodarem na plataforma.
•	Prevenção contra Sobrecarga (Catalyst Optimizer): Em vez de entupir o seu banco iterando coluna por coluna (o que gera "explosões de plano" demoradas), agrupamos todas as criações matemáticas de uma só vez numa única instrução de lista (.select(*expr_list)). Isso torna a compilação instantânea.
•	Broadcast Joins para Performance: Quando vamos cruzar bases pequenas (como Cadastros) contra trilhões de linhas financeiras, aplicamos o encapsulamento broadcast(). Nós enviamos ativamente a tabela menor pela rede de uma só vez para os nossos nós de processamento, eliminando o tempo de "shuffle" (embaralhamento de rede).
•	A Entrega Final (Append): Terminado o tratamento, as suas tabelas são "empilhadas" (Append) no modo físico do Delta Lake Microsoft Fabric com perfeição, prontas para as suas análises interativas da camada Gold.
