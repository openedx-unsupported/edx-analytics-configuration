
.PHONY: deps

deps:
	pip install -U pip==7.1.2
	pip install --no-cache-dir -q -r requirements.txt

# Run ansible in local mode so that it runs the emr module in the
# local python which is likely necessary since that is where the
# proper version of boto is installed.
provision.emr: deps
	ansible-playbook --connection local -i 'localhost,' batch/provision.yml -e "$$EXTRA_VARS"

terminate.emr: deps
	ansible-playbook --connection local -i 'localhost,' batch/terminate.yml -e "$$EXTRA_VARS"

# We actually connect to the master node, hence the lack of a local connection.
collect.metrics: deps inventory.refresh
	ansible-playbook -vvvv -u "$$TASK_USER" batch/collect.yml -e "$$EXTRA_VARS" || true

inventory.refresh:
	./plugins/ec2.py --refresh-cache 2>/dev/null >/dev/null

users.update: deps inventory.refresh
	ansible-playbook -u "$$REMOTE_USER" infrastructure/users.yml -e "$$EXTRA_VARS"
