from __future__ import print_function
from pssh.clients import ParallelSSHClient

hosts = ['169.231.234.215','169.231.234.189','169.231.235.242','169.231.234.250','169.231.235.174','169.231.234.185','169.231.234.169','169.231.235.48']
#hosts = ['turing.mnl.ucsb.edu']

client = ParallelSSHClient(hosts)

#INSTALL SPARK ON EACH NODE
'''
commands = ['wget http://apache.mirrors.hoobly.com/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz',
            'mkdir apps',
            'tar -xvzf spark-2.3.1-bin-hadoop2.7.tgz -C apps/',
            'rm spark-2.3.1-bin-hadoop2.7.tgz',
            ]

'''

#ADD PUBLIC KEYS TO INSTANCES

#commands = ['echo " ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC60hIYeLAje793G6WewxUyJSHQP9xzQ6QhlQaMnc3bOdbVF2sSI89Wt5Ktohir9IbwcToY753EcNfKkhxRM0Ujf+WSXbUuVvZw6pR+UKbKwqgyhaaoW0Ef43viOgU2S5HcvkLHTDRlSW1gu9JlZMK3j1rJ7Dj+s90T99H/fWoakDNqT6s+L/p1F15WVcwrWCiY3nPlkBoOH8696YMb2MmiVKFr4i2gdBX/4setbb/Jok94f0Eq+xrvPM/oMTkxOxC9mL+M1gy8PoD/GuUX9y/Uxco/rORoU5BYF1CHjTwJrGTOXFBLDxGsW59evZHFbir1Q7iIvBbfbRu3Cezu0HrV fhopp@euca-10-1-4-251" >> /home/fhopp/.ssh/authorized_keys']

commands = ['echo " ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCxD7f+4A/MtD6M8cdC3F4HtpMOjMXuTZaOOHrwvy0kuML686kcpeWJm852Re1Iso14t3VRuyaqpr4dct878Au5ISrkQh9CeoKJ+Oi6ygTbnPMuxTV9BFlvwb1DQtqWYWFC10eAiuw3Z57UriiklZOW/uKeOnI1zjIIb1960Q3+oRVkXm/b3kXRBaQOxOc48nFS7Rncu/4YMVT0WqPZcPzIQjcCd5VJG+DBGGj7DTkaeB1A2h2RiPtlUmtB7kw2wZ5+dxCle4kNP1qSZ6jg9dynOYsj1pVxT0Hb+Wpm0HkOd7pdaLUztM9WspxyxylL+ElmCHrTJJohjl1SVSzFsIEz js@hopper.mnl.ucsb.edu >> /home/mona/.ssh/authorized_keys']


#GENERIC EXECUTION SCRIPT
for command in commands:
    output = client.run_command(command)
    for host, host_output in output.items():
        for line in host_output.stdout:
            print(line)



