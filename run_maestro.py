import subprocess
import time

import sys
process = subprocess.Popen(
    [sys.executable, 'maestro.py', '--resume', '0001'],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1
)

# Send /run command
process.stdin.write('/run\n')
process.stdin.flush()

# Read output until it finishes or stalls
while True:
    line = process.stdout.readline()
    if not line:
        break
    print(line, end='')
    if "Passo 4 concluído" in line or "falhou" in line or "Erro" in line:
        # If it seems stuck or finished a major part, we might want to quit
        if "Passo 13" in line or "concluído" in line or "falhou" in line:
            process.stdin.write('/quit\n')
            process.stdin.flush()
            break

process.stdin.close()
process.wait()
