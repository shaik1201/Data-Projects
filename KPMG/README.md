# **Data Analytics Consulting Virtual Internship Experience - KPMG AU**

Tool : Python <br>
Visualization : Tableau <br>
Source Dataset : [KPMG](https://www.theforage.com/modules/m7W4GMqeT3bh9Nb2c/S3uFvbDL49EA43ukg?ref=Mx49trZJgFSC9W2ih)


## 📂 **Overview**

KPMG is a global organization of independent professional firms that provides a range of services to organizations across various industries, government, and non-profit sectors. Its service areas include Audit, Assurance & Risk Consulting; Deals, Tax & Legal; Management Consulting; and Innovation & Digital Solutions.

Under the KPMG Digital Solutions service, as part of the Data, Analytics & Modeling team, we will effectively analyze data sets to help Sprocket Central Pty Ltd, a bike and bike accessories company, develop and optimize its marketing strategies.

The client provided KPMG with 3 datasets:
- Customer demographics
- Customer addresses
- Transactions<br>
<br>

---

## 📂 **Task 1 - Data Quality Assessment**

### **Background Information**

Sprocket Central Pty Ltd needs help with its customer and transactions data. The organisation has a large dataset relating to its customers, but their team is unsure how to effectively analyse it to help optimise its marketing strategy. 

*"The importance of optimising the quality of customer datasets cannot be underestimated. The better the quality of the dataset, the better chance you will be able to use it drive company growth."*

The client provided KPMG with 3 datasets:
- Customer Demographic 
- Customer Addresses
- Transactions data in the past 3 months <br>
<br>

### **Objectives**
- Review and evaluate the data based on Standard Data Quality Dimensions.
- Identify strategies to mitigate that issues.<br>
<br>

### **Result**
[Check out the analysis in the notebook!](https://github.com/faizns/VIX-Data-Analytics-KPMG-AU/blob/main/Task%201%20-%20KPMG%20VIX.ipynb) <br>


| **Tabel Name**      | **No of Records** | **Distinct customer_id** | **Date Data Recieved** |
|---------------------|-------------------|--------------------------|------------------------|
| CustomerDemographic | 4000              | 4000                     | 2022-12-20             |
| CustomerAddres      | 3999              | 3999                     | 2022-12-20             |
| Transactions        | 20001             | 3494                     | 2022-12-20             |

<br>

#### 1. Accuracy : correct values
There are several columns with values that are inaccurate with the existing dataset.
- In **CustomerDemographic**, `DOB` is less able to provide insight into the dataset. From this, `DOB` has been converted to a new column **`Age`**.
- In **Transactions**, the values of `product_first_sold` date have Unix timestamp format. It has been converted to a **general timestamp**.<br>
    <br>

#### 2. Completeness : data field with values
Various columns have empty or missing values in certain records.
- In **CustomerDemographic**, the total percentage of **missing values reaches 30%** with the columns that have the highest percentage of `job_industry_category` and `job_title`. Based on the categorical data type and the distribution of values in each column, they have been **filled with the previous value**.
- In **Transactions**, the percentage of **missing values is only 2.78%**. These records are **still safe to remove** from the dataset, as they do not significantly affect the analysis or modeling results. <br>
<br> 

#### 3. Consistency : value free from contradiction
There are inconsistent datatypes and values for the same column.
- Datatype: **Remove unwanted characters and change the datatype** (e.g. object to numeric). Make sure that the same column in different tables has the same datatype.
    - `DOB`, `transaction_date` is recommended to be datetime.
    - `transaction_id`, `product_id`, `customer_id`, `product_first_sold_date `is recommended to be integer.
    - `standard_cost` is recommended to be float.
- Values: Replace the inconsistent value that has the least frequency of expression.
    - Column `gender` in the CustomerDemographics - "F", "Femel", to **"Female"**; "M" to **"Male"**.
    - Column `state` in the CustomerAddress - "New South Wales" to **"NSW"**, "Victoria" to **"VIC"**. <br>
    <br>

#### 4. Currency : value up to date
- In `deceased_indicator`, **CustomerDemographic** table, value **"Y" ware not current customers** and has been deleted because we want only live customers.<br>
<br>

#### 5. Relevancy : data items with value meta-data
- The `default` in the CustomerDemographic and `Unnamed: 13 to Unnamed: 23` in the **Transaction** table are columns that have no relevance to the dataset. They should be **deleted**.
- In **CustomerDemographic**, the **"U"** values in gender are not known what they represent. So they have been replaced based on the data distribution, **"Male"**.<br>
<br>

#### 6. Validity : data containing allowable values
- In the Transaction table, `standard_cost` has a **value that does not match** the format and **inconsistent** data entry. Remove unwanted characters and change the datatype accordingly.
- There are some records in `product_id = 0`, we can make sure and check the database if there is 0 is a number code of product.<br>
<br>

#### 7. Uniqueness : record that are duplicated
- No duplicate records<br>
<br>

---

## 📂 **Task 2 - Data Insights**

### **Background Information**
Sprocket Central Pty Ltd marketing team is looking to boost business by analysing their existing customer dataset to determine customer trends and behaviour. Using the existing 3 datasets (Customer demographic, customer address and transactions) as a labelled dataset, we will recommend which of these 1000 new customers should be targeted to drive the most value for the organisation. <br>
<br>

### **Objective**
- Built customer segmentation based on RFM model
- Analyzing customer trends, behaviour, and demographic<br>
<br>

### **Result**
[Check out the analysis in the notebook!]() <br>

#### **RFM Analysis**
RFM is a basic customer segmentation algorithm based on their purchasing behavior. The behavior is identified by using only three customer data points: recency, frequency, and monetary. In this project we use Transactions tabel.
- The **recency** value of each customer is obtained from the **smallest recency value from the dataset**.
- The **frequency** value of each customer is obtained from the **count of transactions** they place.
- The **monetary** value of each customer is obtained from the **profit**.

<br>
<p align="center">
    Table 1 — Sample Result of RFM Score and Segment Table <br>
  <kbd> <img width="800" alt="sample tabel rfm" src="https://user-images.githubusercontent.com/115857221/223326532-f01b5c76-691c-491f-9b8e-dcf1a9c82a69.png"> </kbd><br>
</p>
<br>

We calculating the overall RFM score based on:
- **Concatenation**: creating segments, here we just concatenate (not add) individual RFM scores like strings and get labeled segments in return. The highest is 555 and the lowest is 111.
- **Average**: creating score, here we find the average of the individual RFM scores indicating the customer's score. Highest 5 and lowest 1 and we can use this to create more human friendly labelled categories (**Diamond, Platinum, Gold, Silver, Bronze**)

<br>
<p align="center">
  <kbd> <img width="800" alt="newplot (3)" src="https://user-images.githubusercontent.com/115857221/223327109-28089ef6-642e-4db6-b200-d31f8f0ba96e.png"> </kbd> <br>
    Figure 1 — RFM Plot Based on Cluster Score
</p>

From the above plot, we can see how the customers are spread among Recency, Frequency and Monetary dimension. Customers in cluster **Diamond (blue)** have **made recent purchases with a high frequency, and high amounts (monetary)**. On the other hand, customers in Bronze (yellow) have low recency and monetery and haven't purchased for long time.<br>
<br>


<br>
<p align="center">
  <kbd> <img width="500" alt="percent" src="https://user-images.githubusercontent.com/115857221/223327833-5b3f7bf7-2247-41fc-8619-228232338bc3.png"></kbd><br>
    Figure 2 — Percentage of Total Customers and Monetary Based on Cluster Score
</p>

**Most of customers are Platinum** with a percentage of profit contribution (monetary) of 27%. Customers who **contribute the most to generating the highest profit are Diamond**, which is 39%. But, the number of Diamond is low enough which is 17%.<br>

<br>
<p align="center">
    Table 2 — Interpretation Result
</p>

|Customer Score|%|Most RFM Segment|RFM Interpretation|Actionable Insight|
|---|---|---|---|---|
|Diamond|17%|50% Champions; 47% Loyal Customers|Customers who transacted recently, buy often, with a high or low amount of monetary spending. The majority of Diamond customers are Champions and Loyal Customers.| To retain champion customers, companies can reward them. And to convert Loyal Customers into Champion customers, the company should engage them more frequently, ask for reviews, or upsell higher-value products.|
|Platinum|30%|35% Loyal Customers; 27% Potential Loyalists|Customers who made their last transaction some time ago, spend a good amount and purchase more than once or often. RFM segmentation shows that 35% of them are Loyal Customers and 27% are Potential Loyalists.| Companies should offer loyalty programs with benefits like points or discounts for customers and recommend other products to them.|
|Gold|28%|32% At Risk|Customers who spend big money and purchase often but haven't purchased for a long time. RFM segmentation in Gold customer shows that majority 32% of them are At Risk|Companies should bring them back by sending personalized emails or newsletters to reconnect and offer promotions or discounts.|
|Silver|20%|66% Hibernating|Customers who made their last transaction a long time ago, and who have made few purchases. RFM segmentation shows that majority of Silver customers are Hibernating.|Offer other personalized or relevant products, and give more special discount.|
|Bronze|6%|100% Hibernating|Can identify as a lost customer. Lowest recency, frequency, and monetary scores.| Companies can revive interest with outreach campaigns or ignore them otherwise.|
<br>

#### **Demographic Analysis for New Customer Marketing Targeting**
We focused on analyzing the Diamond Customers because they are the most generated profit for organization.

<br>
<p align="center">
  <kbd> <img width="500" alt="age gender" src="https://user-images.githubusercontent.com/115857221/223329095-1fd75486-fc28-4f24-b983-fab353c8bd93.png"></kbd> <br>
    Figure 3 —  Most customers are in the age class 40 - 50, the highest frequency of Diamond customers is in the age range 36 - 55. From this graph, it can also be seen that gender has an almost equal distribution.
</p>
<br>

<br>
<p align="center">
  <kbd> <img width="500" alt="state" src="https://user-images.githubusercontent.com/115857221/223329175-8f30d523-9a6c-4c57-bbfb-b1d68906a8e8.png"> </kbd> <br>
    Figure 4 — The majority of Diamond customers are from New South Wales.
</p>
<br>

<br>
<p align="center">
  <kbd> <img width="500" alt="weatlh state" src="https://user-images.githubusercontent.com/115857221/223329230-4b3e5009-9acc-4218-9686-2d910d0c288b.png"> </kbd> <br>
    Figure 5 — Most of the customers come from Mass Customers, which is twice as much as the total Affluent Customers/High Net Worth. From this graph it can also be seen that NSW also still has a high value.
</p>
<br>

<br>
<p align="center">
  <kbd> <img width="500" alt="properti valuation" src="https://user-images.githubusercontent.com/115857221/223329307-608d17c8-0b9c-4448-9282-0748b84f3709.png"> </kbd> <br>
    Figure 6 — Most Diamond customers have property valuation in range 7 - 10.
</p>
<br>

<br>
<p align="center">
  <kbd> <img width="700" alt="distribution" src="https://user-images.githubusercontent.com/115857221/223329279-2bbe0af0-e89a-4bcb-86c4-520e56f53f72.png"></kbd> <br>
    Figure 7 — Most of Diamond customers are work in manufacturing, financial services, and health industry.
</p>
<br>

Based on the analysis, potential customers who have high value for the organisation are:
- Aged between **35 - 55**
- Work in **manufacturing, financial services, or health industry**
- Classified as **mass customer**
- Live in **New South Wales**
- Have **property valuation at 7 - 10**

From these criteria above, we have **668** out of 1000 new customers who can be targeted for marketing strategies and potentially generate revenue for the company.

---