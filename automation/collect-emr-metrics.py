import json
import requests
import boto3
from sys import exit
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


def get_cluster_region():
  """
  Gets the AWS region this cluster is running in.
  """
  node_az = query_metadata_service('placement/availability-zone')
  # node_az is the full AZ identifier i.e. us-east-1d so we want to take everything
  # but the last character, which gives us the region.  Historically, we have no reason
  # to believe that AZs will start to look like us-east-1tp or anything, so I think
  # this is a reasonably safe bet.
  if node_az is not None:
    return node_az[:-1]

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
  try:
    with open('/mnt/var/lib/info/job-flow.json', 'r') as job_flow_file:
      parsed = json.loads(job_flow_file.read())
      job_flow_id = parsed.get('jobFlowId', None)
  except:
    print "[get-counters] Error reading job flow information.  Not on AWS/EMR?"

  return job_flow_id


def get_cluster_name():
  """
  Gets the name of the EMR cluster.
  """
  cluster_id = get_job_flow_id()
  if cluster_id is not None:
    try:
      emr = boto3.client('emr', region_name=get_cluster_region())
      cluster = emr.describe_cluster(ClusterId=cluster_id)

      return cluster['Cluster']['Name']
    except:
      print "[get-counters] Error querying EMR API for cluster name"
      return None

  return None


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

  Important note: if a tag/piece of context is null, but part of a template, it will get dropped.  That is to say,
  for someone running this not on EMR, we'd be missing the cluster name, job flow ID, etc.  When the metric is
  formatted, these would initially be 'None.' for each of those spots, but we remove those entries. If our template
  was:

    edx.analytics.emr.{cluster}.{flow}.hadoop.counters.foo.bar

  it would end up as:

    edx.analytics.emr.hadoop.counters.foo.bar

  instead.
  """
  output_metrics = []

  for metric, data in metrics.iteritems():
    # Merge our context with any tags for the metric.
    merged_context = context.copy()
    merged_context.update(data.get('context', {}))
    merged_context['metric'] = metric

    # For each "template", feed it in the context so it can be rendered, giving us our transformed metric name.
    data_value = data.get('value', 0)
    for template in templates:
      faceted_metric = template.format(**merged_context)
      faceted_metric = faceted_metric.replace('None.', '')
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

  jobs = response.data.get('jobs', {})
  if jobs is None:
    print "[get-counters] HistoryServer indicates no jobs have run."
    exit(0)

  job_visits = {}
  jobs = jobs.get('job', [])
  for job in jobs:
    # Grab details about the job so we get the full name.
    job_id = job['id']
    job_info = hs.job(job_id)
    job_info = job_info.data['job']

    job_name = get_job_name_from_luigi_task_id(job_info['name'])
    print "[get-counters] Found job '{}' (id: {})".format(job_name, job['id'])

    # Adjust job name to include a zero-based counter if we've seen it before.  This is kind of weird because
    # it only makes "sense" in the context of a given application i.e. to differentiate Sqoop import tasks
    # from one another, and it may change over time (size of list and thus the indexes) and so be not AS useful
    # for over-time analysis, but it suffices because the indexing will be accurate within a single job flow.
    job_index = job_visits.get(job_name, 0)
    job_visits[job_name] = job_index + 1

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
            'context': {
              'job_id': job['id'],
              'job_name': job_name,
              'job_index': job_index
            }
          }


    return facet_metrics(formatted_metrics, metric_templates, context)


if __name__ == "__main__":
  # Define a list of metric name templates, which also us to take in a generated metric,
  # and generate a new version, or versions, based on the template.
  metric_templates = [
    'edx.analytics.emr.{cluster_name}.{job_flow_id}.{job_name}.{job_index}.{metric}'
  ]

  metrics = collect_metrics(metric_templates)
  print "Total metrics: {}".format(len(metrics))
  print json.dumps(metrics)
