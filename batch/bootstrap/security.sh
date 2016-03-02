#!/bin/bash
# https://alas.aws.amazon.com/ALAS-2015-473.html
set -ex

check_vulnerability() {
  cat > glibc_check.c << EOF
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define CANARY "in_the_coal_mine"

struct {
  char buffer[1024];
  char canary[sizeof(CANARY)];
} temp = { "buffer", CANARY };

int main(void) {
  struct hostent resbuf;
  struct hostent *result;
  int herrno;
  int retval;

  /*** strlen (name) = size_needed - sizeof (*host_addr) - sizeof (*h_addr_ptrs) - 1; ***/
  size_t len = sizeof(temp.buffer) - 16*sizeof(unsigned char) - 2*sizeof(char *) - 1;
  char name[sizeof(temp.buffer)];
  memset(name, '0', len);
  name[len] = '\0';

  retval = gethostbyname_r(name, &resbuf, temp.buffer, sizeof(temp.buffer), &result, &herrno);

  if (strcmp(temp.canary, CANARY) != 0) {
    puts("vulnerable");
    exit(EXIT_SUCCESS);
  }
  if (retval == ERANGE) {
    puts("not vulnerable");
    exit(EXIT_SUCCESS);
  }
  puts("should not happen");
  exit(EXIT_FAILURE);
}
/* from http://www.openwall.com/lists/oss-security/2015/01/27/9 */
EOF
  gcc glibc_check.c -o glibc_check
  ./glibc_check
}

amazonlinux_patch() {
  echo "Apply patch for AmazonLinux"
  sudo yum update -y glibc bash
  sudo /etc/init.d/sshd restart
}

debian_patch() {
  echo "Apply path for Debian"
  sudo apt-get clean
  sudo mv /etc/apt/sources.list /tmp/sources.list.bk
  sudo sh -c 'echo "deb http://http.us.debian.org/debian wheezy main contrib non-free" >>  /etc/apt/sources.list'
  sudo sh -c 'echo "deb http://security.debian.org wheezy/updates main contrib non-free" >>  /etc/apt/sources.list'
  sudo apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --force-yes --only-upgrade libgcc1 bash
  sudo mv /tmp/sources.list.bk /etc/apt/sources.list
  sudo apt-get clean
  sudo /etc/init.d/ssh restart
  sudo mv /etc/apt/sources.list /tmp/sources.list.bk
  cat > /tmp/sources.list.lts << EOL
deb http://http.us.debian.org/debian squeeze-lts main contrib non-free
deb http://security.debian.org squeeze/updates main contrib non-free
EOL
  sudo cp /tmp/sources.list.lts /etc/apt/sources.list
  sudo apt-get clean
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --force-yes libmysqlclient-dev python-dev libssl-dev libxml2-dev libxslt1-dev
  cat > /tmp/sources.list.new << EOL
deb http://http.us.debian.org/debian wheezy main contrib non-free
deb http://security.debian.org wheezy/updates main contrib non-free
EOL
  sudo mv /tmp/sources.list.new /etc/apt/sources.list
  sudo apt-get clean
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --force-yes libatlas3gf-base libffi-dev libpq-dev
  sudo mv /tmp/sources.list.lts /etc/apt/sources.list
}

main() {
  set +e
  apt-get -V &>/dev/null
  local ec=$?
  set -e
  if [ $ec -eq 0 ]; then
    debian_patch
  else
    amazonlinux_patch
  fi
  [ "$(check_vulnerability)" == "not vulnerable" ]
}

main
