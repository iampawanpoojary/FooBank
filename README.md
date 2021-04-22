# FooBank


- *main.py* contains the python script
- *Foobank.ipynb* contains the same code but with data visualized
- Html version of the jupyter notebook is included
- *foobank_beam_pipeline.py* consists of unfinished apache beam pipeline which i thought would be fun to do, but did not have enough time to finish. However its a dataflow pipeline which does a left join (cogroupbykey) on loan and customer data (sample data in code), and stores them into bigquery.

*To run the dataflow pipeline*
```
python -m foobank_beam_pipeline.py 
--runner DataflowRunner 
--project=pawpooja 
--region=europe-west1 
--staging_location=gs://pawpooja/test 
--temp_location gs://pawpooja/tmp/

```
*dataflow execution graph*
![alt text](https://github.com/iampawanpoojary/FooBank/blob/main/images/beam.png)

*bigquery output*
![alt text](https://github.com/iampawanpoojary/FooBank/blob/main/images/bq.png)

