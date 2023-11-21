# ssh_tunnels
 
Uses the sfapi to start a new job and then opens an ssh tunnel to the compute node you're running on.

To use, get a valid sfapi key from iris.nersc.gov and a valid ssh key from sshproxy.sh.
```
git clone https://github.com/tylern4/perlmutter_ssh_tunnel.git
cd perlmutter_ssh_tunnel
pip install -r requirements.txt
python src/run.py submit -f test.sh -p 9000
```