CREATE EXTERNAL TABLE OTIF (
    Sales_Order_Document long,
    Sales_Organization int,
    Article string,
    Size_Grid_Value int,
    `Year` int,
    `MONTH` int,
    OTIF_KPI double
)
USING delta
PARTITIONED BY (Sales_Organization,`Year`,`MONTH`)
location "C:\\Users\\singhays\\Projects\\CaseStudy\\tables\\otif"