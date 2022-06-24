
.PHONY: requirements upgrade pin-pip

COMMON_CONSTRAINTS_TXT=requirements/common_constraints.txt
.PHONY: $(COMMON_CONSTRAINTS_TXT)
$(COMMON_CONSTRAINTS_TXT):
	wget -O "$(@)" https://raw.githubusercontent.com/edx/edx-lint/master/edx_lint/files/common_constraints.txt || touch "$(@)"

upgrade: export CUSTOM_COMPILE_COMMAND=make upgrade
upgrade: $(COMMON_CONSTRAINTS_TXT)
	## update the requirements/*.txt files with the latest packages satisfying requirements/*.in
	pip install -qr requirements/pip-tools.txt
	pip-compile --allow-unsafe --rebuild --upgrade -o requirements/pip-tools.txt requirements/pip-tools.in
	pip-compile --upgrade -o requirements/pip-tools.txt requirements/pip-tools.in
	pip install -qr requirements/pip.txt
	pip install -r requirements/pip-tools.txt
	pip-compile --upgrade -o requirements/base.txt requirements/base.in
	# Post process all of the files generated above to replace the instructions for recreating them
	script/post-pip-compile.sh \
        requirements/pip-tools.txt \
        requirements/base.txt

pin-pip:
	pip install -qr requirements/pip.txt

requirements: pin-pip
	pip install --no-cache-dir -q -r requirements/base.txt

# Run ansible with --connection local but WITHOUT -i 'localhost,' so that it
# runs the emr module in the local python which is likely necessary since that
# is where the proper version of boto is installed.
provision.emr: requirements
	ansible-playbook --connection local batch/provision.yml -e "$$EXTRA_VARS"

terminate.emr: requirements
	ansible-playbook --connection local batch/terminate.yml -e "$$EXTRA_VARS"

inventory.refresh:
	./plugins/ec2.py --refresh-cache >/dev/null

users.update: requirements inventory.refresh
	ansible-playbook -u "$$REMOTE_USER" infrastructure/users.yml -e "$$EXTRA_VARS"
