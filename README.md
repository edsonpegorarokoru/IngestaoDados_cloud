### Introdução <br>
Conforme solicitado, nesse trabalho criamos uma aplicação que se comunica com a api da Polygon.io trazendo informações de cotações/índices, enviando os dados coletados a um banco de dados do Google BigQuery.<br>

Para que esses dados fossem inseridos na base da Big Query criada, usamos a aplicação Apache Airflow, que executa o arquivo produzido em python, e a cada 20 minutos envia a uma tabela desse dataset.<br>

### API Polygon.io
Para utilização da API precisamos de um import:
```
from polygon import RESTClient
```
E também chamar a id da api:
```
client = RESTClient('4HntDYSlAIfaSCgx_EAYVBb24ijvMpdu')
```






 <br>

### Declarações de problemas
Olist coletou dados ao longo do ano, os dados contêm informações separadas, existem dados agregados, dados únicos ou mesmo uma combinação de ambos (agregados em dados únicos). Com o conjunto de dados olist atual, é difícil identificar o comportamento do cliente, uma vez que não há representação dos segmentos de clientes, além disso, mesmo depois de entender com segmentação que tipo de abordagem eles deveriam adotar? daí... As perguntas a serem respondidas neste projeto:

1. Existe alguma tendência de crescimento nas vendas do comércio eletrônico no Brasil? <br>
2. Como estão concentradas as vendas totais nos estados brasileiros? <br>
3. Quais são os melhores estados para fazer transações no Brasil? <br>
4. Como os tipos de pagamentos podem influenciar o comércio eletrônico? <br>
5. Como é o comportamento do nosso cliente? <br>
6. Como você os classificaria? <br>
7. Como a estratégia de campanha de CRM será implementada? <br>

### Objetivo
Com este projeto espera-se que a Olist consiga classificar os seus clientes com a segmentação adequada.

### Limitação do Projeto
A analise de dados feita, é limitado a pedidos de 2016 a 2018 feitos em vários mercados no Brasil a partir do conjunto de dados públicos de comércio eletrônico brasileiro da Olist. Portanto, o desempenho do modelo será muito diferente quando usado para classificar os clientes de hoje. #Além disso, esta analise foi construída a partir de vários recursos selecionados com base na importância dos recursos e na análise de correlação com o alvo.

### Abordagem analítica
Subjacente às declarações do problema, nosso foco principal está na implementação da campanha de CRM, a abordagem analítica que usamos é:

1. Segmentação de clientes por clustering <br>
2. Classificação de Clusters <br>

### Métricas de avaliação
Por lidar com classificação multiclasse, as Falsas Classes são igualmente importantes, portanto, o F1-Score seria uma métrica apropriada para isso. <br>

## Data Understanding <br>
![Data Sets Scheme](Images/data_schemeArtboard_1.png) <br>
Dataset Resource: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_order_items_dataset.csv

Descrição dos recursos:

- order_id: identificador único do pedido.
- order_item_id: número sequencial que identifica a quantidade de itens incluídos no mesmo pedido.
- product_id: identificador exclusivo do produto.
- seller_id: identificador exclusivo do vendedor.
- Shipping_limit_date: Mostra a data limite de envio do vendedor para o envio do pedido ao parceiro logístico.
- preço: preço do item.
- frete_valor: item de valor do frete do item (se um pedido tiver mais de um item o valor do frete é dividido entre os itens).
- customer_id: chave para o conjunto de dados de pedidos. Cada pedido possui um customer_id exclusivo.
- order_status: Referência ao status do pedido (entregue, enviado, etc).
- order_purchase_timestamp: mostra o carimbo de data/hora da compra.
- order_approved_at: mostra o carimbo de data e hora de aprovação do pagamento.
- order_delivered_carrier_date: mostra o carimbo de data e hora de postagem do pedido. Quando foi entregue ao parceiro logístico.
- order_delivered_customer_date: mostra a data real de entrega do pedido ao cliente.
- order_estimated_delivery_date: Mostra a data estimada de entrega que foi informada ao cliente no momento da compra.
- payment_sequential : um cliente pode pagar um pedido com mais de um método de pagamento. Se ele fizer isso, será criada uma sequência para acomodar todos os pagamentos.
- payment_type: forma de pagamento escolhida pelo cliente.
- payment_installments: quantidade de parcelas escolhida pelo cliente.
- payment_value: valor da transação.
- review_id: identificador exclusivo da revisão.
- review_score: Nota que varia de 1 a 5 dada pelo cliente em pesquisa de satisfação.
- review_comment_title: Título do comentário da avaliação deixada pelo cliente, em português.
- review_comment_message: Mensagem de comentário da avaliação deixada pelo cliente, em português.
- review_creation_date: Mostra a data em que a pesquisa de satisfação foi enviada ao cliente.
- review_answer_timestamp: mostra o carimbo de data e hora da resposta da pesquisa de satisfação.
- product_category_name: categoria raiz do produto, em português.
- product_name_lenght: número de caracteres extraídos do nome do produto.
- product_description_lenght: número de caracteres extraídos da descrição do produto.
- product_photos_qty: número de fotos publicadas do produto.
- product_weight_g: peso do produto medido em gramas.
- product_length_cm: comprimento do produto medido em centímetros.
- product_height_cm: altura do produto medida em centímetros.
- product_width_cm: largura do produto medida em centímetros.
- customer_unique_id: identificador único de um cliente.
- customer_zip_code_prefix: primeiros cinco dígitos do CEP do cliente.
- customer_city: nome da cidade do cliente.
- customer_state: iniciais do estado do cliente.
- geolocation_lat_x: latitude do cliente.
- geolocation_lng_x: longitude do cliente.
- seller_zip_code_prefix: primeiros cinco dígitos do CEP do vendedor.
- seller_city: nome da cidade do vendedor.
- seller_state: iniciais do estado do vendedor.
- geolocation_lat_y: latitude do vendedor.
- geolocation_lng_y: longitude do vendedor.
- product_category_name_english: nome da categoria em inglês.

## Análise Exploratória de Dados <br> <br>

### Evolução do total de pedidos no Brazilian E-Commerce
![output1](Images/output1.png) <br>
Insights:
- Não há registro em novembro de 2016 e registros incompletos em setembro de 2018. O maior número de pedidos recebidos em 1 mês é em novembro de 2017.
- Com base no total de pedidos por dia da semana, segunda e terça são as que mais transações ocorreram em uma semana, com 16,29% e 16,05% do total.
- No total de pedidos por horário do dia, verifica-se que a tarde é o maior número de transações realizadas em um dia com 38,29% do total. Isso pode acontecer porque mais clientes têm tempo livre durante os intervalos para almoço. A segunda maior é durante a noite, enquanto os clientes têm mais tempo livre depois de um dia inteiro de trabalho.

### Comparação do total de pedidos entre 2017 e 2018 (janeiro a agosto)
![output2](Images/output2.png) <br>
Insights:
- Com base no gráfico de comparação acima, pode-se observar que houve um aumento nos pedidos de 136,52% entre janeiro e agosto de 2017 e janeiro e agosto de 2018. Isso pode acontecer porque muitos novos clientes estão realizando transações no Olist em 2018. Portanto, este caso deve ser investigado mais detalhadamente na modelagem.

### Evolução dos pedidos de comércio eletrônico nas regiões brasileiras
![output3](Images/output3.png) <br>
Insights:
- Com base no gráfico Evolução dos pedidos de comércio eletrônico nas regiões brasileiras, verifica-se que o Brasil está dividido em 5 grandes regiões, onde o Sudeste é a região com a evolução mais rápida dos pedidos de comércio eletrônico de janeiro de 2017 a agosto de 2018. Enquanto isso, São Paulo e Rio de Janeiro da região Sudeste também estão em 1º e 2º lugar nos maiores pedidos em cada estado e cidade.

### Evolução do E-commerce: Total de Pedidos e Valor Total Vendido em R$
![output4](Images/output4.png) <br>
Insights:
- O maior valor vendido da história foi em novembro de 2017. É possível que isso aconteça quando for realizado o evento blackfriday, onde cada loja provavelmente dá um desconto enorme que só acontece por 1 dia. O valor é 1001,9K apenas por 1 mês.
- O valor total entre janeiro e agosto de 2017 a janeiro e agosto de 2018 também aumentou 137,04%. Onde a evolução do valor médio do frete tende a ser estável de janeiro de 2017 a maio de 2018. Um aumento acentuado ocorreu em julho de 2018. Isso pode ter acontecido porque muitos novos clientes passam a usar o Olist como seu e-commerce diário para fazer compras.

### Estudo Comparativo: Desempenho de Entrega de Comércio Eletrônico
![output5](Images/output5.png) <br>
Insights:
- O valor médio do frete pago é de R$ 22,77. Mas, ainda há muitos estados que devem pagar mais caro por frete com valor acima de R$ 40. Os cinco primeiros são Paraíba, Roraima, Rondônia, Acre e Maranhão.
- Até mesmo o estado de Roraima também está entre os 2 primeiros dos 5 estados com maior tempo médio de entrega. Isso precisa ser mais discutido entre a equipe Olist, para que o valor do frete possa ser ajustado e cada cliente tenha o melhor serviço de entrega pelo preço certo.

### Evolução das formas de pagamento no Brazilian E-Commerce
![output6](Images/output6.png) <br>
Insights:
- Com base no gráfico 3 acima, verifica-se que o cartão de crédito é a opção de pagamento preferida pelos clientes com 73,99% do total. A evolução das formas de pagamento com cartão de crédito também está aumentando rapidamente de janeiro de 2017 a maio de 2018. Muitos clientes também preferiram parcelar o pagamento em 1 mês. Isso indica que no pagamento com cartão de crédito, o cliente não precisa pagar imediatamente o valor integral dos produtos que compra. Eles podem pagar nas parcelas que preferirem e podem ser pagos no final dos períodos.
- O maior número de parcelas que o cliente prefere é 1. Indica que muitos clientes compram produtos que não são muito caros e ainda podem ser pagos no final do primeiro mês.

## Agrupamento
![output6](Images/cluster.png) <br>

### Com base no resultado do clustering, podemos concluir:

1. Classe 2: alta monetária e baixa atualidade
- Cliente recente e com alto gasto
- Ainda fresco com alta confiança para gastar mais
- Este tipo de cliente é o cliente que queremos reter, é o nosso melhor cliente

2. Classe 0: alta monetária e alta atualidade
- Um de nossos antigos clientes
- Mas tenha confiança para gastar alto
- Tipo de cliente que passa pelo nosso e-commerce quando quer comprar produtos de alto valor

3. Classe 3: Baixa monetária e baixa atualidade
- Clientes frescos
- Não gasta tanto quanto a classe 0 e a classe 2
- Tipo de cliente que adora fazer compras e comprar pequenas coisas

4. Classe 1: Baixa monetária e alta atualidade
- Clientes que estão experimentando nosso e-commerce
- Teve um gasto baixo
- O tipo de cliente que experimenta o nosso e-commerce e pode acabar não gostando

### Classificação
![feature](Images/featureimportance.png) <br>

A partir das descobertas, podemos concluir que o rótulo do cluster está alinhado com a explicação dos valores SHAP, que são:

1. Classe 2: alta monetária e baixa atualidade
- Cliente recente e com alto gasto
- Ainda fresco com alta confiança para gastar mais
- Este tipo de cliente é o cliente que queremos reter, é o nosso melhor cliente

2. Classe 0: alta monetária e alta atualidade
- Um de nossos antigos clientes
- Mas tenha confiança para gastar alto
- Tipo de cliente que passa pelo nosso e-commerce quando quer comprar produtos de alto valor

3. Classe 3: Baixa monetária e baixa atualidade
- Clientes frescos
- Não gasta tanto quanto a classe 0 e a classe 2
- Tipo de cliente que adora fazer compras e comprar pequenas coisas

4. Classe 1: Baixa monetária e alta atualidade
- Clientes que estão experimentando nosso e-commerce
- Teve um gasto baixo
- O tipo de cliente que experimenta o nosso e-commerce e pode acabar não gostando

## Conclusão e recomendação prática

### Conclusão

- Há uma tendência crescente nas vendas do comércio eletrônico no Brasil quando o valor total vendido entre 2017 a 2018 aumenta para 137,04%. O maior valor vendido da história foi em novembro de 2017, com 1.001,9 mil apenas por 1 mês. Isso pode acontecer porque o evento blackfriday é realizado naquele mês, onde cada vendedor provavelmente dá um desconto enorme que só acontece por 1 dia.

- O maior preço médio por estado cliente é a Paraíba com R$ 215, mas o preço do valor tende a ser baixo com apenas R$ 113,40 mil. Inversamente proporcional a São Paulo onde o preço médio é menor, mas teve o maior preço em relação a outros estados com R$ 5.172,26 mil. É assim que se concentram as vendas totais no Brasil.

- O melhor estado para fazer transações no Brasil é São Paulo, onde este estado domina o total de pedidos de todos os estados brasileiros em 42,8% do total.

- O cartão de crédito é a opção de pagamento preferida pelos clientes com 73,99% do total. A evolução das formas de pagamento com cartão de crédito também está aumentando rapidamente de janeiro de 2017 a maio de 2018. Muitos clientes também preferiram parcelar o pagamento em 1 mês. Isso indica que no pagamento com cartão de crédito, muitos clientes não precisam pagar imediatamente o valor integral dos produtos que compraram. Eles podem pagar nas parcelas que preferirem e podem ser pagos no final dos períodos. Os vouchers também podem ser utilizados para pagamentos, com isso o cliente pode obter descontos nos produtos que comprou e adicionar mais produtos para comprar. É assim que os tipos de pagamento podem influenciar o comércio eletrônico.

- Com base nos clusters, concluímos que está dividido por 4 grupos de clientes, sendo eles:

1. Classe 2: O melhor cliente, gasta mais em um cronograma recente
2. Classe 0: Tipo de cliente com confiança em gastar com produtos de alto valor, mas pode não escolher este e-commerce com gastos baixos
3. Classe 3: Tipo de cliente que adora comprar pequenas coisas
4. Classe 1: tipo de cliente com alto potencial de rotatividade

### Limitação do modelo
A limitação do modelo é uma condição em que o modelo previu aleatoriamente a observação e provavelmente classificou incorretamente os rótulos de verdade. Nesta seção, tentamos descobrir as previsões que provavelmente foram classificadas incorretamente pelo modelo com base nos valores SHAP, filtrando as observações com valores SHAP insignificantes dos valores total_price e total_freight.

### Recomendação sobre desempenho do modelo
1. Modelo adicional que não foi utilizado:
- Para clustering: Agglomerative Hierarchy Clustering, DBscan, Gaussian Mixture Model, BIRCH
- Para classificação: VotingClassifer, StackingClassifier, SVC, Naive Bayes, KNN

2. Melhoria no conjunto de dados, recursos mais explicáveis, como dados demográficos do cliente.

3. Mais modelagem ou solução, como vendas de recursos (análise de série temporal), análise de sentimento do cliente, sistema de recomendação

### Recomendação para o problema de negócios
*Classe 2: Melhor Cliente*

Meta: Reter e, se possível, aumentar<br>
Como ?
- Fazer promoção em todos os itens inclui produtos de menor ou maior valor.
- Fornece pontos e recompensas para incentivar compras repetidas.

*Classe 0: Cliente Antigo de Alto Valor:*

Objetivo: Atrair Interesse <br>
Como ?
- Foco na retenção de clientes, dando descontos em itens de alto valor.
- Fornecer excelente atendimento ao cliente e garantir uma experiência de compra perfeita para suas transações de alto valor.
- Garantia de entrega no prazo, entrega atrasada resultará em frete grátis na próxima compra.
- Criar uma experiência apelativa e user-friendly para o cliente.
- Oferecer melhores serviços de remessa, através do rastreamento ao vivo da remessa.

*Classe 3: Comprador Fresco:*

Meta: Aumentar os Gastos <br>
Como ?
- Implementar estratégias para aumentar o valor médio dos pedidos, oferecendo ofertas em pacote.
- Faça uma promoção pela primeira vez, como frete grátis por um mês inteiro.
- Fornece um processo de compra detalhado com carrinho de compras fácil de usar e múltiplas opções de pagamento.
- Mostrar recomendação de produto por compra anterior.

*Classe 1: Potenciais Churners:*

Meta: Atrair Interesse e Aumentar Gastos <br>
Como ?
- Reengajamento do cliente através da promoção de um usuário antigo para que ele volte a usar o e-commerce.
- Colete feedback desses clientes para entender suas preocupações ou problemas e resolvê-los prontamente.

## Caso de negócios
Supondo que temos um problema de negócio, onde precisamos fazer campanhas de marketing para aumentar o valor monetário dos clientes e também reduzir custos na realização de campanhas e ofertas, **assumindo o caso**:

1. temos um total de 750 clientes
2. cerca de 250 novos clientes aderiram
3. receita total de 500.000 dólares

> Sem modelo:

Sem modelo só haverá campanha cega, sem saber a quem abordar, ou mesmo como abordar. eles acabarão usando todos os orçamentos da campanha, ou até mais, então, no final:

- com o total de 1000 clientes
- cada cliente recebe uma promoção com desconto de cerca de 50 dólares
- orçamento total utilizado: 50.000 dólares e pode gastar mais

> Com modelo:

Com o clustering podemos identificar nossos clientes com base em segmentos, desta forma podemos direcionar a campanha, e como exemplo que de 1.000 clientes 750 deles são o mercado alvo, mas neste cenário só chegamos até os segmentos e quem abordagem, além disso, o modelo de cluster usado é baseado apenas em RFM, portanto eles são muito limitados, então no final:

- 750 clientes são os únicos abordados
- orçamento esperado utilizado: 37.500 dólares

e com base na classificação a previsão do cluster é fortemente determinada pela taxa de entrega e também pelo preço do produto

com isso todos os 750 têm situação diferente, onde 250 do cliente tem gastos altos com alta taxa de entrega, e 500 do cliente tem gastos altos e baixo custo de entrega, com isso:

- o cliente 250 ainda precisa do desconto integral de 50 dólares
- o cliente 500 não precisa do desconto total pois já tem baixo custo de entrega, o que leva a uma diminuição do custo, e assumindo que para o custo de entrega é 25, isso significa que o cliente 500 só precisa de 25 dólares como custo

Então, no final:

- 250 clientes abordados com desconto total
- 750 clientes abordados com desconto parcial
- (250 x 50) + (500 x 25) = 12.500 + 12.500 = 25.000 dólares

Portanto, ao usar o modelo, a empresa economizou metade do orçamento de marketing.

















