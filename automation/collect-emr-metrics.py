import json
import requests
import boto3
from yarn_api_client import HistoryServer


def query_metadata_service(path=''):
  """
  Gets the value at the specified path by querying the instance metadata serice.

  Returns the textual response and does not attempt to deserialize the response body.
  """
  try:
    r = requests.get('http://169.254.169.254/latest/meta-data/' + path)
    return r.text
  except Exception as e:
    print "[get-counters] exception when querying instance metadata service for path '{}': {}".format(path, e)
    return None


def get_local_address():
  """
  Gets the local/private IPv4 address of this instance.
  """
  return query_metadata_service('local-ipv4')


def get_instance_id():
  """
  Gets the instance ID of this instance.
  """
  return query_metadata_service('instance-id')


def get_job_flow_id():
  """
  Gets the job flow ID that this instance belongs to.
  """
  job_flow_id = None
  with open('/mnt/var/lib/info/job-flow.json', 'r') as job_flow_file:
    parsed = json.loads(job_flow_file.read())
    job_flow_id = parsed.get('jobFlowId', None)

  return job_flow_id


def get_cluster_name():
  """
  Gets the name of the EMR cluster.
  """
  cluster_id = get_job_flow_id()

  emr = boto3.client('emr')
  cluster = emr.describe_cluster(ClusterId=cluster_id)

  return cluster['Cluster']['Name']


def get_context():
  """
  Gets contextual information about where the script is being run.

  This includes things like the instance ID, and in the cases of special environments like EMR, the job flow ID.
  """
  return {
    'instance_id': get_instance_id(),
    'job_flow_id': get_job_flow_id(),
    'cluster_name': get_cluster_name(),
  }


def get_job_name_from_luigi_task_id(task_id):
  """
  Gets the name of the job based on the task ID that Luigi uses.

  This is usually in the form of TaskClassName(param=value, param=value, ...) and so we take the simple approach
  of chopping off whatever is in front of the parameters tuple.
  """
  return task_id.split('(')[0]


def get_prefix_from_counter_group_name(group_name):
  """
  Gets the simplified version of a counter group name, suitable for use with Graphite.

  Normally, Hadoop counter groups have names which correspond to the fully qualified Java class name
  which emitted the counters e.g. org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter.

  We're simply grabbing the last component in the full string, and then running it through a mapping
  to generate a simpler version.
  """
  group_name = group_name.split('.')[-1]

  class_name_to_simple_map = {
      'FileSystemCounter': 'fs',
      'JobCounter': 'job',
      'TaskCounter': 'task',
      'Shuffle Errors': 'shuffle',
      'FileInputFormatCounter': 'file_input',
      'FileOutputFormatCounter': 'file_output'
  }

  group_name = class_name_to_simple_map.get(group_name, group_name)

  # Last ditch effort to handle cases not covered here and attempt to pass a viable metric name back.
  group_name = group_name.replace(' ', '_').lower()

  return 'hadoop.counters.' + group_name


def facet_metrics(metrics, templates, context={}):
  """
  Facets the given metrics by creating copies based on the supplied templates.

  This function takes a dictionary of metric name -> metric value mappings, and for each of those metrics,
  will generate a new metric name (with the same value) by iterating through the list of templates and using
  both the original metric name and the context dictionary (tags, mostly, like instance ID of job flow ID)
  to format a new metric name.

  Thus, with a metric of "my.lil.metric", with the tag "foobar" having a value of "quux", and a template of
  "{foobar}.{metric_name}", we'd get "quux.my.lil.metric".
  """
  output_metrics = []

  for metric, data in metrics.iteritems():
    # Merge our context with any tags for the metric.
    merged_context = context.copy()
    merged_context.update(data.get('tags', {}))
    merged_context['metric_name'] = metric

    # For each "template", feed it in the context so it can be rendered, giving us our transformed metric name.
    data_value = data.get('value', 0)
    for template in templates:
      faceted_metric = template.format(**merged_context)
      output_metrics.append((faceted_metric, data_value))

  return output_metrics


def collect_metrics(metric_templates):
  """
  Collects Hadoop counters from all jobs on the local HistoryServer, transforming them for forwarding
  to Graphite, along with any configured faceting.
  """
  # Get our local IP (the private IP of the instance) because not all of the YARN services
  # run on localhost.  Also, even though this library claims to be able to read the Hadoop
  # configure files to find out where things are running, it doesn't seem to work fully.
  local_address = get_local_address()

  print "[get-counters] Targeting Hadoop on local address '{}'".format(local_address)

  # Load up the context for where we're running.
  context = get_context()

  print "[get-counters] Context for this run:"
  for (k,v) in context.iteritems():
    print "                 {} => {}".format(k, v)

  # Grab all jobs from the history server.
  hs = HistoryServer(local_address)
  response = hs.jobs()
  for job in response.data.get('jobs', {}).get('job', []):
    # Grab details about the job so we get the full name.
    job_info = hs.job(job['id'])
    job_info = job_info.data['job']

    job_name = get_job_name_from_luigi_task_id(job_info['name'])
    print "[get-counters] Found job '{}' (id: {})".format(job_name, job['id'])

    job_counters = hs.job_counters(job['id'])
    job_counters = job_counters.data['jobCounters']

    formatted_metrics = {}
    for counter_group in job_counters.get('counterGroup', []):
      metric_prefix = get_prefix_from_counter_group_name(counter_group['counterGroupName'])

      for metric in counter_group.get('counter', []):
        # Pull out the counter value for both the map and reduce stages, as well as the total.
        metric_name = metric['name'].lower()
        for type in ['total', 'map', 'reduce']:
          metric_type = type + 'CounterValue'
          metric_value = metric[metric_type]
          full_metric_path = '.'.join([metric_prefix, metric_name, type])

          formatted_metrics[full_metric_path] = {
            'value': metric_value,
            'tags': {
              'job_name': job_name
            }
          }


    return facet_metrics(formatted_metrics, metric_templates, context)


if __name__ == "__main__":
  # Define a list of metric name templates, which also us to take in a generated metric,
  # and generate a new version, or versions, based on the template.
  metric_templates = [
    'edx.analytics.emr.{cluster_name}.{job_flow_id}.{instance_id}.{job_name}.{metric}'
  ]

  print json.dumps(collect_metrics(metric_templates))
