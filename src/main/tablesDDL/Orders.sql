CREATE EXTERNAL TABLE orders (
    Sales_Order_Document long,
    Sales_Organization int,
    Article string,
    Size_Grid_Value int,
    Requested_Delivery_Date date,
    Sales_Order_Qty int,
    `Year` int,
    `MONTH` int
)
USING delta
PARTITIONED BY (Sales_Organization,`Year`,`MONTH`)
location "C:\\Users\\singhays\\Projects\\CaseStudy\\tables\\orders"