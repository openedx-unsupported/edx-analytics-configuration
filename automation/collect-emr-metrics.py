import json
import requests
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


def get_context():
  """
  Gets contextual information about where the script is being run.

  This includes things like the instance ID, and in the cases of special environments like EMR, the job flow ID.
  """

  return {
    'instance_id': get_instance_id(),
    'job_flow_id': get_job_flow_id()
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

  return 'hadoop.counters.' + group_name


def main():
  # Get our local IP (the private IP of the instance) because not all of the YARN services
  # run on localhost.
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

    formatted_job_counters = {}
    for counter_group in job_counters.get('counterGroup', []):
      metric_prefix = get_prefix_from_counter_group_name(counter_group['counterGroupName'])

      for metric in counter_group.get('counter', []):
        # Pull out the counter value for both the map and reduce stages, as well as the total.
        metric_name = metric['name'].lower()
        for type in ['total', 'map', 'reduce']:
          metric_type = type + 'CounterValue'
          metric_value = metric[metric_type]
          full_metric_path = '.'.join([metric_prefix, metric_name, type])

          formatted_job_counters[full_metric_path] = metric_value

    print json.dumps(formatted_job_counters)


if __name__ == "__main__":
  main()
