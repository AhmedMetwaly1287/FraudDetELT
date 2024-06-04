<h1>Fraud Transactions EL, ETL Pipeline & Analysis</h1>

<h2>Project Details</h2>

<p>The project demonstrates the creation of a Extract-Load Pipeline using Apache Kafka and Extract-Transform-Load Pipeline using Apache Spark and the analysis of 50 thousand Fraud Transactions and the probable cause of fraud transactions, the categories and the organizations impacted most by it.</p>

<h3>Tools Used</h3>

<ol>

<li>
    <strong>Apache Kafka 2.8.2</strong>
</li>

<li>
    <strong>MySQL:</strong>
    <ul>
        <li>Used as the data storage system.</li>
        <li>Hosts the transformed data and the additional tables created for analysis.</li>
    </ul>
</li>

<li>
    <strong>Python Libraries (PySpark, PyMySQL, Kafka-Python):</strong>
    <ul>
        <li>PySpark is used for interfacing with Apache Spark through Python, faciliates the process of Data Transformation and Wrangling.</li>
        <li>PyMySQL allows for interaction with the MySQL database, creation of a table and loading the transformed data inside of it.</li>
        <li>Kafka-Python as an interface to interact with the underlying Kafka Server, subscribing to the created topic, a producer to load the data, stream it to the kafka-server to be consumed by consumer and loaded into the MySQL database.</li>
    </ul>
</li>

<li>
    <strong>Microsoft Power BI:</strong>
 <ul>
        <li>Employed for more advanced data visualizations.</li>
        <li>Facilitates deeper insights by creating interactive reports and dashboards.</li>
</ul>
</li>
</ol>

<h3>To-Do:</h3>
<ul>
    
Refactor the code to be more optimized

Use Different Tool for Producers/Consumers

Enhance the Data Visualization to create a more interactive, in depth and dynamic dashboard

</ul>

<h3>Results of Data Visualization</h3>

![image](https://github.com/AhmedMetwaly1287/FraudDetELT/assets/139663311/84a075bc-e52b-4521-88a5-6ae6188c7344)

![Screenshot 2024-03-06 174334](https://github.com/AhmedMetwaly1287/FraudDetELT/assets/139663311/f34cd279-04bc-4dc5-a838-6e7044227c19)



