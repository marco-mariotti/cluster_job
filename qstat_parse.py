#! /usr/bin/env python2.7
from string import *
import sys
from commands import *
from MMlib import *
### allowing this to be piped without weird python complains
import signal
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def_opt= {'temp':'/users-d3/mmariotti/temp', 
          'i':0, 'o':0, 'v':0,
          'f':            '{id:9} {state:3} {slots:3} {tasks:6} {queue:12} {node:18} {name}',
          'fa':'{owner:12} {id:9} {state:3} {slots:3} {tasks:6} {queue:12} {node:18} {name}',          
          'q':0, 'a':0, 'x':''}

help_msg="""qstat_parse.py: Utility to visualize the output of qstat -xml in a convenient way. 
For a fresh qstat run, run this program without input.
To read text from an input, use: 
 -i inputfile    (use "-i -" for standard input) 

 -f provides the output format as a python string interpreted by .format() 
    These attributes of each job can be used: 
    id name state   owner   priority   queue   node   tasks
    Default: """ + def_opt['f'] + """

-x  if qstat is run (no -i provided), use this to provide any qstat options
-a  add option ' -u "*" ' to qstat, to display jobs for all users. If -f is not provided, owner is also displayed
-q  display queue usage instead; simply wraps 'qstat -g c'
"""

command_line_synonyms={}


#########################################################
###### start main program function

class job (object):
  """ """

def main(args={}):
#########################################################
############ loading options
  global opt
  if not args: opt=command_line(def_opt, help_msg, 'io', synonyms=command_line_synonyms )
  else:  opt=args
  set_MMlib_var('opt', opt)
  #global temp_folder; temp_folder=Folder(random_folder(opt['temp'])); test_writeable_folder(temp_folder, 'temp_folder'); set_MMlib_var('temp_folder', temp_folder)
  #global split_folder;    split_folder=Folder(opt['temp']);               test_writeable_folder(split_folder); set_MMlib_var('split_folder', split_folder) 

  # queueu usage mode
  if opt['q']:
    write(bbash('qstat -g c '+opt['x']), 1)
    sys.exit()
  
  ## setting input. if none, running qstat
  output_format=opt['f']
  if opt['i']==0:
    
    qstat_options=opt['x']
    if opt['a']:
      qstat_options+=' -u \"*\" '
      if output_format==def_opt['f']: output_format=opt['fa']
    qstat_cmd='qstat -xml'+ qstat_options
    input_file_h=bash_pipe(qstat_cmd)  ## opening pipe
    
  elif opt['i']!='-':
    global input_file;   input_file=opt['i'];   check_file_presence(input_file, 'input_file')
    input_file_h=open(input_file, 'r')
  else:
    input_file_h=sys.stdin

  for c in input_file_h:
    c=c.strip()
    if '<job_list' in c:
      j=job()
      j.tasks=''
      j.slots=''
    elif '<JB_job_number' in c:      j.id=c.split('>')[1].split('<')[0]
    elif '<JAT_prio'  in  c:      j.priority=c.split('>')[1].split('<')[0]
    elif '<JB_name'  in  c:      j.name=c.split('>')[1].split('<')[0]    
    elif '<JB_owner'  in  c:      j.owner=c.split('>')[1].split('<')[0]    
    elif '<state>'  in  c:      j.state=c.split('>')[1].split('<')[0]    
    elif '<queue_name>' in c:   
      j.queue = c.split('>')[1].split('<')[0].split('@')[0]
      try: j.node = c.split('>')[1].split('<')[0].split('@')[1]
      except: j.node=''
    elif '<tasks' in c:
      j.tasks = c.split('>')[1].split('<')[0]
    elif '<slots' in c:
      j.slots = c.split('>')[1].split('<')[0]

    if '</job_list>' in c:   
      #exec(opt['a'])
      write(output_format.format(**j.__dict__), 1)

    
  input_file_h.close()

  ###############



#######################################################################################################################################

def close_program():
  if 'temp_folder' in globals() and is_directory(temp_folder):
    bbash('rm -r '+temp_folder)
  try:
    if get_MMlib_var('printed_rchar'): 
      printerr('\r'+printed_rchar*' ' ) #flushing service msg space       
  except:
    pass

  if 'log_file' in globals(): log_file.close()


if __name__ == "__main__":
  try:
    main()
    close_program()  
  except Exception:
    close_program()
    raise 
