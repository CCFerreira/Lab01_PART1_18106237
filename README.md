## Arquitetura
CSV → Python → Parquet → PostgreSQL

## Etapas

### Bronze
Ingestão dos dados sem tratamento.

### Silver
- Limpeza de dados
- Conversão de tipos
- Remoção de nulos e duplicados
- Geração de Parquet

### Gold
Carga dos dados no PostgreSQL para análise.

## Dicionário de Dados

| Coluna | Tipo | Descrição |
|--------|------|----------|
| order_id | string | ID do pedido |
| customer_id | string | ID do consumidor |
| order_purchase_timestamp | timestamp | Data/hora da compra  |
| order_approved_at | string | Data/hora da aprovação da compra  |
| order_delivered_carrier_date | string |Data/hora da entrega ao "correio"|
| order_delivered_customer_date | string | Data/hora da entrega ao consumidor  |
| order_estimated_delivery_date | string | Data/hora da esxpectativa de entrega ao consumidor  |

## Qualidade de Dados

- Presença de valores nulos removidos
- Conversão de datas realizada
- Dados duplicados eliminados

## Querys de Negócios
✔ Pedidos por status

SELECT status, COUNT(*)
FROM fato_pedidos
GROUP BY status;

delivered	96455
canceled	6

✔ Pedidos por mês
SELECT t.ano, t.mes, COUNT(*)
FROM fato_pedidos f
JOIN dim_tempo t ON f.data = t.data
GROUP BY t.ano, t.mes
ORDER BY t.ano, t.mes;
ano	    mes	count
2016	9	1
2016	10	270
2016	12	1
2017	1	748
2017	2	1641
2017	3	2546
2017	4	2303
2017	5	3545
2017	6	3135
2017	7	3872
2017	8	4193
2017	9	4149
2017	10	4478
2017	11	7288
2017	12	5513
2018	1	7069
2018	2	6556
2018	3	7003
2018	4	6798
2018	5	6749
2018	6	6096
2018	7	6156
2018	8	6351

✔ Horário de pico
SELECT t.hora, COUNT(*)
FROM fato_pedidos f
JOIN dim_tempo t ON f.data = t.data
GROUP BY t.hora
ORDER BY COUNT(*) DESC;
hora count
16	6474
11	6384
14	6383
13	6307
15	6248
21	6040
20	6008
10	5978
17	5959
19	5802
12	5800
22	5656
18	5585
9	4647
23	4014
8	2906
0	2322
7	1199
1	1132
2	496
6	477
3	259
4	203
5	182

✔ 4. Horário com mais pedidos
SELECT 
    t.hora,
    COUNT(*) AS total_pedidos
FROM fato_pedidos f
JOIN dim_tempo t ON f.data = t.data
GROUP BY t.hora
ORDER BY total_pedidos DESC;

hora total_pedidos

16	6474
11	6384
14	6383
13	6307
15	6248
21	6040
20	6008
10	5978
17	5959
19	5802
12	5800
22	5656
18	5585
9	4647
23	4014
8	2906
0	2322
7	1199
1	1132
2	496
6	477
3	259
4	203
5	182

✔ 5. Crescimento por ano
SELECT 
    t.ano,
    COUNT(*) AS total_pedidos
FROM fato_pedidos f
JOIN dim_tempo t ON f.data = t.data
GROUP BY t.ano
ORDER BY t.ano;

ano	total_pedidos
2016	272
2017	43411
2018	52778

## Como executar

```bash
pip install -r requirements.txt

python scripts/bronze.py
python scripts/silver.py
python scripts/gold.py