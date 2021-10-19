import argparse
import apache_beam as beam
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.textio import WriteToText
from apache_beam.io.gcp.datastore.v1new.types import Query

from dict2xml import dict2xml


def get_properties(row):
    return row.properties

def print_row(row):
    print(row)
    return row
    
def convert_to_xml(row):
    return dict2xml(row, wrap = 'User', indent = '  ')

def create_query(kind, project, namespace):
    return Query(kind, project, namespace)

def dataflow_pipeline(pipeline_options, query, output, num_shards, header):
    p = beam.Pipeline(options=pipeline_options)
    datastore_to_gcs_xml = ( p 
    | 'Read from datastore' >> ReadFromDatastore(query)
    | 'Extract data' >> beam.Map(get_properties)
    | 'Convert to XML' >> beam.Map(convert_to_xml)
    | 'Print to console' >> beam.Map(print_row)
    | 'Write to file' >> WriteToText(output, num_shards=num_shards, header=header)
    )
    result = p.run()
    result.wait_until_finish()

def run(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--kind', dest='kind', type=str, required=True, help='Datastore kind')
    parser.add_argument('--namespace', dest='namespace', type=str ,default=None ,help='Datastore namespace, no need to specify in case of default namespace')
    parser.add_argument('--output', dest='output', required=True, type=str, help='Output file to write results to')
    parser.add_argument('--num_shards', dest='num_shards', type=int, default=1, help='Number of output shards')
    parser.add_argument('--project', dest='project', required=True, type=str, help='Google cloud project name with id')
    parser.add_argument('--region', dest='region', default='us-east1', type=str, help='Region of google cloud')
    parser.add_argument('--job_name', dest='job_name', default='unique-job-name', help='Job name for current run')
    parser.add_argument('--runner', dest='runner', default='DirectRunner', help='Runner for pipeline')
    parser.add_argument('--header', dest='header', default='<HEADER>', help='Header for xml file')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(
        pipeline_args,
        runner = known_args.runner, 
        project = known_args.project,
        job_name = known_args.job_name, 
        region = known_args.region
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    project = pipeline_options.view_as(GoogleCloudOptions).project
    query = create_query(known_args.kind, project, known_args.namespace)
    dataflow_pipeline(pipeline_options, query, known_args.output, known_args.num_shards, known_args.header)

if __name__ == '__main__':
    run()