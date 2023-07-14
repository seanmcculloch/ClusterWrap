from shutil import which
import os
from .clusters import allen_slurm, local_cluster

cluster = local_cluster

if which('sbatch') is not None:
    if os.system('sbatch -V') != 32512:
        cluster = allen_slurm


