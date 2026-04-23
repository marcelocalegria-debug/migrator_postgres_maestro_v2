 Maestro e os scripts de migração estão preparados para rodar em Linux, mas a forma como ele encontra a
  biblioteca do Firebird é diferente e muito mais simples que no Windows.

  Aqui está como funciona no Linux:

  1. Busca Automática (Padrão do SO)
  Diferente do Windows, onde o Python precisa que você aponte o caminho exato do .dll, no Linux a biblioteca fdb utiliza
  o mecanismo padrão de carregamento de bibliotecas dinâmicas do sistema (ld).

  Se você instalou o cliente do Firebird (pacote firebird3.0-common ou similar), o Linux coloca o arquivo libfbclient.so
  em diretórios como /usr/lib/ ou /usr/local/lib/. O Python encontrará isso automaticamente sem que precisemos de
  nenhuma linha de código extra de "auto-discovery".

  2. O que você precisa garantir no Linux
  Para que funcione perfeitamente, você deve ter o cliente do Firebird instalado. Em distribuições baseadas em
  Debian/Ubuntu, você faria:

   1 sudo apt-get update
   2 sudo apt-get install firebird3.0-common  # ou libfbclient2

  3. Como o código trata isso
  Se você olhar o início de scripts como migrator_v2.py ou o fix_blob_text_columns.py que acabei de ajustar, verá que as
  proteções de DLL estão dentro de blocos if os.name == 'nt':.

  No Linux (os.name == 'posix'), o código simplesmente faz:

   1 import fdb
   2 # fdb vai procurar por libfbclient.so nos paths padrão do sistema

  4. E se a biblioteca estiver em um lugar customizado?
  Caso você tenha o Firebird instalado em um diretório não padrão (ex: /opt/firebird/lib), basta adicionar esse caminho
  à variável de ambiente LD_LIBRARY_PATH antes de rodar o maestro:

   1 export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/firebird/lib
   2 python maestro.py

  Resumo: No Linux é "instalar e usar". O sistema operacional gerencia a localização da biblioteca para o Python de
  forma transparente.