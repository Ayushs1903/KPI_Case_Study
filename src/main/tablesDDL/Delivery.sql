CREATE EXTERNAL TABLE delivery (
    Delivery_Note_Header long,
    Sales_Order_Document long,
    Sales_Organization int,
    Actual_Delivery_Date date,
    Delivery_Qty int,
    `Year` int,
    `MONTH` int
)
USING delta
PARTITIONED BY (Sales_Organization,`Year`,`MONTH`)
location "C:\\Users\\singhays\\Projects\\CaseStudy\\tables\\delivery"