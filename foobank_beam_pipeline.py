# python -m foobank_beam_pipeline.py --runner DataflowRunner --project=pawpooja --region=europe-west1 --staging_location=gs://pawpooja/test --temp_location gs://pawpooja/tmp/
# TODO:
# created source IO for gcs
# ptransforms for csv formatting/cleaning 

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import logging

PROJECT='pawpooja'
BUCKET='pawpooja'


class FooBankMergeData(beam.PTransform):
    """this ptransform performs a left join on loan and customer data"""

    def __init__(self, loan_pipeline_name, loan_data, customer_pipeline_name, customer_data, key):
        self.customer_pipeline_name = customer_pipeline_name
        self.loan_data = loan_data
        self.loan_pipeline_name = loan_pipeline_name
        self.customer_data = customer_data
        self.key = key

    def expand(self, pcolls):
        def format_key(data_dict, key):
            return data_dict[key], data_dict

        return ({pipeline_name: pcoll | 'Convert to ({0}, object) for {1}'
                .format(self.key, pipeline_name)
                                >> beam.Map(format_key, self.key)
                 for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestKey(),
                                                   self.loan_pipeline_name,
                                                   self.customer_pipeline_name)
                )


class UnnestKey(beam.DoFn):
    """this dofn class unnests the CogroupBykey output """

    def process(self, input_element, loan_pipeline_name, customer_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[customer_pipeline_name]
        source_dictionaries = grouped_dict[loan_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


class LogContents(beam.DoFn):
    """This dofn class logs the content of that which it receives """

    def process(self, input_element):
        logging.info("foobank customers: {}".format(input_element))
        logging.info("file type: {}".format(type(input_element)))
        logging.info("loan['id']: {}".format(input_element['id']))
        return


def run(argv=None):
    """entry point for pipeline"""
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)

    # Create Example read Dictionary data
    loan_pipeline_name = 'loan_data'
    loan_data = p | 'create loan data' >> beam.Create(
        [
                {
                "id": "20427847",
                "name": "pawan",
                "ssn": "172-10-3586",
                "birthday": "1978-05-09",
                "gender": "F",
                "city": "mumbai",
                "zip_code": "76378"
                },
                {
                "id": "74075652",
                "name": "john",
                "ssn": "140-57-5668",
                "birthday": "1994-03-05",
                "gender": "F",
                "city": "malmo",
                "zip_code": "75665"
                },
                {
                "id": "20427847",
                "name": "pawan",
                "ssn": "172-10-3586",
                "birthday": "1978-05-09",
                "gender": "F",
                "city": "mumbai",
                "zip_code": "76378"
                },
                {
                "id": "74075652",
                "name": "john",
                "ssn": "140-57-5668",
                "birthday": "1994-03-05",
                "gender": "F",
                "city": "malmo",
                "zip_code": "75665"
                }
            ]
         )
    customer_pipeline_name = 'customer_data'
    customer_data = p | 'create customer data' >> beam.Create(

                    [
                        {
                        "id": "9546870",
                        "user_id": "20427847",
                        "timestamp": "2017-10-28 03:42:29",
                        "loan_amount": "80000",
                        "loan_purpose": "Buying a pet",
                        "outcome": "ACCEPTED",
                        "interest": "8.7",
                        "webvisit_id": "72572646"
                        },
                        {
                        "id": "36469880",
                        "user_id": "74075652",
                        "timestamp": "2017-10-24 23:35:34",
                        "loan_amount": "270000",
                        "loan_purpose": "Refinance existing loans",
                        "outcome": "REJECTED",
                        "interest": "6.7",
                        "webvisit_id": "72572646"
                        },
                        {
                        "id": "22554526",
                        "user_id": "20427847",
                        "timestamp": "2017-10-03 20:59:49",
                        "loan_amount": "240000",
                        "loan_purpose": "Home purchase",
                        "outcome": "ACCEPTED",
                        "interest": "7",
                        "webvisit_id": "54404642"
                        },
                        {
                        "user_id": "27906548",
                        "id": "74075652",
                        "timestamp": "2017-10-22 08:23:47",
                        "loan_amount": "190000",
                        "loan_purpose": "Buying a pet",
                        "outcome": "REJECTED",
                        "interest": "14",
                        "webvisit_id": "72572646"
                        }
                    ]
         
         )

    key = 'id'
    pipelines_dictionary = {loan_pipeline_name: loan_data,
                            customer_pipeline_name: customer_data}
    test_pipeline = (pipelines_dictionary
                     | 'FooBank Merge Data' >> FooBankMergeData(
                loan_pipeline_name, loan_data,
                customer_pipeline_name, customer_data, key)
                     | 'Log data' >> beam.ParDo(LogContents())
                     | 'Write to BigQuery' >> beam.io.Write(
                            beam.io.BigQuerySink(
                                # The table name is a required argument for the BigQuery sink.
                                # In this case we use the value passed in from the command line.
                                'foobank_dataset.foo_bank',
                                # Here we use the simplest way of defining a schema:
                                # fieldName:fieldType
                                schema='id:STRING,name:STRING,ssn:STRING,birthday:STRING,gender:STRING,city:STRING,zip_code:STRING,user_id:STRING,timestamp:STRING,loan_amount:STRING,loan_purpose:STRING,outcome:STRING,interest:STRING,webvisit_id:STRING',
                                # Creates the table in BigQuery if it does not yet exist.
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                # Deletes all data in the BigQuery table before writing.
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

                     )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()

