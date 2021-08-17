import argparse
import json
import os
import requests
import subprocess
import xml.etree.ElementTree as ET

def run_shell_command(cmd):
    process = subprocess.run(cmd.split(" "))
    assert process.returncode == 0

def checkout_repo(repo, branch):
    run_shell_command(f'git clone http://github.com/{repo}')
    os.chdir(repo.split('/')[-1])
    run_shell_command(f'git checkout {branch}')
    os.chdir("..")


# Get plugin repo and branch from command line
parser = argparse.ArgumentParser(description='Run CDAP end-to-end tests.')
parser.add_argument('plugin_repo', metavar='REPO', type=str, action='store',
                   help='Github repository where the plugin lives. ex: data-integrations/google-cloud')
parser.add_argument('plugin_branch', metavar='BRANCH', type=str, action='store',
                   help='Github branch name. ex: develop')
args = parser.parse_args()


# Start CDAP sandbox
print("Pulling sandbox docker image")
run_shell_command("docker pull caskdata/cdap-sandbox:latest")
print("Running sandbox")
run_shell_command("docker run --name cdap-sandbox -p 11011:11011 -p 11015:11015 -d caskdata/cdap-sandbox")

# Checkout the plugin repo
print("Checking out plugin repo")
checkout_repo(args.plugin_repo, args.plugin_branch)

# Build the plugin
print("Building plugin repo")
os.chdir(args.plugin_repo.split('/')[-1])
run_shell_command("mvn clean package -DskipTests")

# Get plugin artifact name and version from pom.xml.
root = ET.parse('pom.xml').getroot()
plugin_name = root.find('{http://maven.apache.org/POM/4.0.0}artifactId').text
plugin_version = root.find('{http://maven.apache.org/POM/4.0.0}version').text

os.chdir("target")
plugin_properties = {}
plugin_parents = []
# Get plugin properties and parent from plugin json.
with open(f'{plugin_name}-{plugin_version}.json') as f:
    obj = json.loads(f.read())
    plugin_properties = obj['properties']
    plugin_parents = obj['parents']

data = None
with open(f'{plugin_name}-{plugin_version}.jar', 'rb') as f:
    data = f.read()

# Install the plugin on the sandbox.
print("Installing plugin")
res=requests.post(f"http://localhost:11015/v3/namespaces/default/artifacts/{plugin_name}", headers={"Content-TYpe": "application/octet-stream", "Artifact-Extends": '/'.join(plugin_parents), "Artifact-Version": plugin_version}, data=data)
assert res.ok or print(res.text)
res=requests.put(f"http://localhost:11015/v3/namespaces/default/artifacts/{plugin_name}/versions/{plugin_version}/properties", json=plugin_properties)
assert res.ok or print(res.texts)

os.chdir("../..")

# Checkout e2e test repo
print("Checking out cdap-e2e-tests")
checkout_repo('cdapio/cdap-e2e-tests', 'develop')

# Run e2e tests
print("Running e2e tests")
os.chdir('cdap-e2e-tests')
run_shell_command("mvn clean test")
