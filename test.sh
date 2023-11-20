#!/bin/bash
#SBATCH -q debug
#SBATCH -A m3792
#SBATCH -t 00:10:00
#SBATCH -C cpu
#SBATCH -J server
#SBATCH -o server.out
#SBATCH -e server.err

module load python
echo "Starting server"

cd $SCRATCH

# Run a basic http server at port 9000
python -m http.server 9000
sleep 300
