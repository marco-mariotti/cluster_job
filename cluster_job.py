#! /usr/bin/python -u
__author__  = "Marco Mariotti"
__email__   = "marco.mariotti@crg.cat"
__licence__ = "GPLv3"
__version__ = "0.2"
from string import *
import sys
import traceback
from commands import *
from MMlib import *
#### default options:
header_template="""#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -M {email} {queue_line}{time_line}{additional_options}
#$ -N {name}
#$ -l virtual_free={mem}G {pe} 
"""
header_single_job=header_template+"""#$ -e {outfile}.LOGERR
#$ -o {outfile}.LOG
"""
header_array_job=header_template+"""#$ -e {outfile}.$TASK_ID.LOGERR
#$ -o {outfile}.$TASK_ID.LOG
#$ -t {range_str}
"""
pe_template="\n#$ -pe smp {procs}"   ## for n of processors; not available in all systems

def_opt= {'i':0,  'o':0, 'n':None, 'c':None, 'H':0, 'qsub':0, 'p':1, 'm':12, 'N':0, 't':6, 'v':0, 'n_lines':0, 'n_jobs':0, 'F':0, 'r':None,
'q':'queue1,queue2', 'bin':'~/bin', 'email':'youremail@domain.com', 'E':'a', 'e':0, 'f':0, 'x':0,
'q_syn':'S=queue1,queue2;L=queue3,queue2' }

help_msg="""Program to split and manage command lines, to write job files to be submitted to a SGE cluster. The input file must contain one line for each bash command that can be parallelized.
By default, an array job file is prepared inside the output folder with one entry for each line in input. Otherwise, the lines are split into a number of job files according to options -n_jobs (-nj) or -n_lines (-nl).
Option -N can be used to give a name to the jobs: if provided argument is "JobZ" for example, job names will be JobZ.1, JobZ.2 etc.
Other options can be used to set SGE job variables, such as the queue name, memory and cpu requirements etc.

Usage #1 (array mode, default)  cluster_job.py input_file [output_folder] [name_prefix]
Usage #2 (clusters of jobs)     cluster_job.py input_file [output_folder] [name_prefix] [-n_jobs N | -n_lines M]

*** Options:
-i      +   input file. Use '-' for standard input
-o      +   output folder. Default is input file name plus ".jbs"
-N      +   define name of jobs with this prefix. Default is input file name

-r     S-E        in array mode, defines range of jobs executed. Wraps qsub option -t. Defaults to all jobs
-n_lines | -nl +  set this to have x lines of input commands in each job. Turns off array mode
-n_jobs  | -nj +  set this to have a number of jobs x. Overrides -n_lines and turns off array mode
When array mode is off, options -n and -c are available to control dynamically job name and content. See advanced help with -h full

-q      +   queue name(s), comma separated; synonyms can be also used. These can be set with -q_syn. See -h default
-m      +   GB of memory requested
-t      +   time limit requested in hours  (sge option -l h_rt)
-p      +   number of processors requested, if smp is available. Use -p 0 to not specify processors

-bin    +   in every job, the PATH variable is set to include this folder before any other
-H      +   file with a header command, that is executed in every job before the input commands
-F      +   file with a footer command, that is executed in every job after the input commands
-e          do not export the current environment variables in the job (do not use option -V in qsub)
-email  +   email provided when submitting job
-E      +   option -m of qsub; send an email in conditions determined by the argument. Multiple arguments can be concatenated, e.g. -E abe
        'b': at the beginning of the job; 'e': end of the job; 'a': if aborted or rescheduled; 's': suspended.

-qsub | -Q  submit the jobs with qsub
-f          force overwrite of jobs folder if existing. By default, it prompts.

-print_opt     prints default values for all options
-h | --help    print this help and exit
               use "-h default" for instructions on how to set default values for cluster_job
               use "-h full" for more usage examples and help on -n nameEXPR and -c jobEXPR
"""

advanced_help="""##### Advanced usage
When job array mode is off (cluster jobs mode), both the name of the jobs and the command lines in the job files can be provided as expressions evaluated in python. In expressions, you manipulate a variable called x, which is the whole line of input file. If the lines in the input file contain tab characters, the fields are also split in attributes of x: x.a, x.b, x.c and so on.
The -c jobEXPR builds each command line included in any job file.
The -n nameEXPR determines the name of the job, thus also its name of the job file in the output folder. Note that if multiple lines per input are included in a job file, the name of the job file will be derived by the last line included.
In the expressions, you can use the variables name (job name), out (job file written), or index (line index in the input file), n_job (the index of the current job file).
Expressions can be combined with input file structures created ad hoc. A simple use of this is a tab separated input file with job name as first field, and the command line as second field. You can use such file with  -n x.a  -c x.b
More examples:

### Example #1: simple usage. Input file content:
 launch_X_application.py   -i  data_piece1  -e
 launch_X_application.py   -i  data_piece2  -e
 launch_X_application.py   -i  data_piece3  -e
Run with:          cluster_job.py   -i input_file  -o X_application  -N Xapp  -nl 1

This will produce three files:
 X_application/Xapp.1  # job name: Xapp.1  # content: launch_X_application.py   -i  data_piece1  -e
 X_application/Xapp.2  # job name: Xapp.2  # content: launch_X_application.py   -i  data_piece2  -e
 X_application/Xapp.3  # job name: Xapp.3  # content: launch_X_application.py   -i  data_piece3  -e

### Example #2:    expressions with tab separated input files. Input file content:
 Homo_sapiens   chr1
 Homo_sapiens   chr2
 Drosophila     chrX
Run with: cluster_job.py -i inputfile -o this_job -n " x.a+ '.' +x.b "   -c " 'echo ' +x.a+ ' and ' +x.b "  -nl 1
                                                     |-- nameEXPR ---|      |-------- jobEXPR -------|
This will produce three files:
 this_job/Homo_sapiens.chr1  # job name: Homo_sapiens.chr1  # content:  echo Homo_sapiens and chr1
 this_job/Homo_sapiens.chr2  # job name: Homo_sapiens.chr2  # content:  echo Homo_sapiens and chr2
 this_job/Drosophila.chrX    # job name: Homo_sapiens.chrX  # content:  echo Drosophila and chrX

### Example #3:     using information contained in each job line to assign the job name. For example, you have an inputfile with commands to scan a number of genomes:
 scan_genome /home/genomes/Drosophila_ananassae/genome.fa
 scan_genome /home/genomes/Drosophila_erecta/genome.fa
 scan_genome /home/genomes/Drosophila_grimshawi/genome.fa
 scan_genome /home/genomes/Drosophila_melanogaster/genome.fa
Run with: cluster_job.py -i inputfile -o g_scan -n  " x.split('/')[-2] "  -nl 1
                                                    |---- nameEXPR ----|
This will produce 4 files:
 g_scan/Drosophila_ananassae    # job name: Drosophila_ananassae    # content:  scan_genome /home/genomes/Drosophila_ananassae/genome.fa
 g_scan/Drosophila_erecta       # job name: Drosophila_erecta       # content:  scan_genome /home/genomes/Drosophila_erecta/genome.fa
 g_scan/Drosophila_grimshawi    # job name: Drosophila_grimshawi    # content:  scan_genome /home/genomes/Drosophila_grimshawi/genome.fa
 g_scan/Drosophila_melanogaster # job name: Drosophila_melanogaster # content:  scan_genome /home/genomes/Drosophila_melanogaster/genome.fa
"""
set_default_help="""######## How to set default values for options
Typically, each user of this program will use cluster_job.py with the same options, adapted to the system.
There are two ways to achieve this.
1. (recommended) With an alias.
Set an alias in your bash environment to point to the default cluster_job.py command line that you want to use. Through this you can also shorten the call. If for example you want call your alias C, you have something like:
$ alias C="cluster_job.py -email youremail@domain.com  -bin ~/your_bin/  -q my_default_queue -m 12 -p 1 -t 6 -q_syn  'A=queue1,queue2;B=queue3,queue4' "
including all options that you want to set as defaults. Note the syntax of option -q_syn to define queue keyword shortcuts. In the example, this allows then to use A as synonym for queue1,queue2 and B as synonym of queue3,queue4.
After this, you call the program with all your custome default values simply with:   C -i input_file [other options]
We suggest to include your custom alias definition in your .bashrc file, so that it will be available in any terminal you open.

2. Editing the script
If you have permissions, you may instead open directly your copy of cluster_job.py and modify it. All default options are defined in the very beginning of the file, through "header" variables, and a dictionary called def_opt.
"""
not_my_email_err="""Hey! get your own -email ;) \n\nHere's the help page to avoid typing this option every time:\n\n"""+set_default_help

command_line_synonyms={'Q':'qsub', 'nj':'n_jobs', 'nl':'n_lines', 'force':'f'}

#########################################################
###### start main program function

class tab_line(str):
  """ container class, gets tab separated fields from a input file"""
  def __init__(self, line=None):
    if line:
      for index, field in enumerate(line.strip().split('\t')):
        setattr(self, lowercase[index], field)
    self.line=line
  def __str__(self): return str(self.line)

def main(args={}):
#########################################################
############ loading options
  global opt
  if not args: opt=command_line(def_opt, help_msg, 'ioN', synonyms=command_line_synonyms, advanced={'full':advanced_help, 'default':set_default_help} )
  else:  opt=args
  set_MMlib_var('opt', opt)

  write('                         [   '); write('cluster_job v'+__version__, how='reverse'); write('   ]                         ', 1)
  #checking input
  global input_file;   input_file=opt['i'];
  if input_file=='-':  input_file_h=sys.stdin
  else:
    check_file_presence(input_file, 'input_file', notracebackException)
    input_file_h=open(input_file)

  if opt['email']=='youremail@domain.com' and bbash('whoami')!='mmariotti' and opt['E']: raise notracebackException, not_my_email_err

  cmd_lines= [ line.strip() for line in input_file_h if line.strip() and not line.strip().startswith("#") ]    ## loading whole file
  # determining number of jobs, number of lines
  tot_lines=len(cmd_lines)
#  single_job_mode=False
  if tot_lines==0:    raise notracebackException, "ERROR input file is empty!"
  if tot_lines==1:
    array_mode=False; n_lines_per_job=1; n_jobs=1
  elif not opt['n_lines'] and not opt['n_jobs']:
    array_mode=True
    #if tot_lines==1: single_job_mode=True
  else:
    array_mode=False
    if opt['r']: raise notracebackException, "ERROR option -r (job range in array) available only in array mode! options -nl and -nj must be inactive (or set to zero) to enter array mode"
    if opt['n_jobs']:   n_lines_per_job= float(  tot_lines ) / int(opt['n_jobs'])
    if opt['n_lines']:  n_lines_per_job= opt['n_lines']
    n_jobs=  (tot_lines-1) / n_lines_per_job +1
    #if n_jobs==1: single_job_mode=True

  global name_e;  name_e=opt['n'] ;    global cmd_e; cmd_e=opt['c']

  output_folder=opt['o']
  #if single_job_mode:
  #  if not output_folder: single_output_file= input_file+'.jb' #....
  if not output_folder:    #raise Exception, "ERROR you must specify an expression for output files (option -o) ; please run -h for more information"
    output_folder= input_file+'.jbs'

  ######### names and commands including expressions
  if not cmd_e is None and array_mode: raise notracebackException, 'ERROR -c cmdEXPR is not available in job array mode. Use either  -nl number_of_lines   or   -nj number_of_jobs  ; please run with -h for more information'
  if not array_mode and cmd_e is None: cmd_e="x"  # standing for: execute full line
  if not name_e is None:
    if array_mode: raise notracebackException, 'ERROR -n nameEXPR is not available in job array mode. Use either  -nl number_of_lines   or   -nj number_of_jobs ; please run with -h for more information'
  else:
    if opt['N']:   prefix_name=opt['N'].rstrip('.')
    else:
      if input_file=='-': raise notracebackException, "ERROR you must specify job name with -N if reading from standard input!"
      prefix_name=base_filename( input_file )
    if not array_mode: name_e='"'+prefix_name+'."+n_job'
  ### at this stage, if array_mode we have prefix_name ; if not array_mode we have name_e and cmd_e

  if is_directory(output_folder):
    if not opt['f']:
      if not raw_input("Jobs folder "+output_folder+" existing from a previous run;  overwrite? will delete previous logs if present [Y] \n") in ['', 'Y', 'y', 'yes']:
        raise notracebackException, "Aborted. "
    bash('rm -r '+output_folder);
  output_folder=Folder(output_folder);
  email= opt['email']

  ### determining header command, present in every job file
  init_command=''
  if opt['bin']:    init_command='export PATH='+opt['bin']+':$PATH\n'
  if opt['H']:      init_command+= join([ line.strip() for line in open(opt['H']) ], '\n')  ##adding header lines
  footer_command=''
  if opt['F']:      footer_command+= join([ line.strip() for line in open(opt['F']) ], '\n')  ##adding footer lines


  ### determining the queue argument.
  queue_synonyms={}
  if opt['q_syn']:
    for assign_piece in opt['q_syn'].split(';'):
      syn_name, queue =assign_piece.split('=') #queue can be comma separated but we pass it as it is
      queue_synonyms[syn_name]=queue
  queue_name=opt['q']
  if queue_name in queue_synonyms: queue_name=queue_synonyms[queue_name]
  if queue_name: queue_line= "\n#$ -q "+queue_name
  else:          queue_line= ""

  ## time constraint
  if opt['t']:   time_line="\n#$ -l h_rt="+str(opt['t'])+":00:00"
  else:          time_line=""

  additional_options=''
  if opt['E']:          additional_options+='\n#$ -m '+opt['E']+' '
  if not opt['e']:      additional_options+='\n#$ -V '
  
  pe_specs=pe_template.format(procs=opt['p']) if opt['p'] else ''
  mem=opt['m']

  def write_job(cmd, name, outfile):
    """ Takes the command, plus all other variables computed and available in namespace, prepares a single job file and submit it if necessary"""
    header=header_single_job.format(email=email, additional_options=additional_options, queue_line=queue_line, 
                                    time_line=time_line, name=name, outfile=outfile, pe=pe_specs, mem=mem)
    write('Writing file: '+outfile)
    write_to_file(header +init_command.rstrip('\n')+'\n'+cmd.rstrip('\n')+'\n'+footer_command, outfile)
    if opt['qsub']:
      write(' \tsubmitting file!')
      bbash('qsub '+outfile)
    write('', 1)

  def write_array_job(cmd_list, name, outfile, range_str):
    """ Takes the command list, plus all other variables computed and available in namespace, prepares an array file and submit it if necessary"""
    header=header_array_job.format(email=email, additional_options=additional_options, queue_line=queue_line, 
                                    time_line=time_line, name=name, outfile=outfile, pe=pe_specs, mem=mem,
                                    range_str=range_str)
    exec_cmd="""awk -v task_id=$SGE_TASK_ID -F"#" 'BEGIN{pat="^#" task_id "# " }$0 ~ pat{system( substr($0, length($2)+4) )} ' """+outfile+'\n'
    out_text=header+init_command.rstrip('\n')+'\n'+ exec_cmd + join([ '#'+str(index+1)+'# '+cmd.rstrip('\n') for index, cmd in enumerate(cmd_list)], '\n' )+     footer_command
    write('Writing array file ('+str(len(cmd_list))+' jobs): '+outfile)
    write_to_file(out_text, outfile)
    if opt['qsub']:
      write(' \tsubmitting file!')
      bbash('qsub '+outfile)
      write('', 1)

  #####################
  if array_mode:
    name=prefix_name
    outfile=abspath(output_folder+name)
    if opt['r']: range_str=opt['r']
    else:        range_str='1-'+str(len(cmd_lines))
    write_array_job(cmd_lines, name, outfile, range_str)
  else:
  ## from here it goes only if we're not in array job mode
  #cycling file ;   producing a "cmd" variable with all the lines to put in a job; then we write (and submit it)
    cmd='';     lines_up_to_now=0;     index=1;     n_job=1
    for cline in cmd_lines:
      x=tab_line(cline.strip())
      index=str(index);       n_job=str(n_job) # converting to str just to allow to use them inside the custom expressions

      try:       name=  eval(  name_e )
      except:
        printerr("Can't evaluate name expression: "+name_e, 1)
        raise

      outfile=abspath(output_folder+name)

      try:       cmd+=  eval(  cmd_e )+'\n'
      except:
        printerr("Can't evaluate command expression: "+cmd_e, 1)
        raise

      lines_up_to_now+=1
      n_job=int(n_job)

      if lines_up_to_now >= n_lines_per_job*n_job: # enough lines. lets' write to a file (and submit)
        write_job(cmd, name, outfile)
        cmd=''
        n_job+=1

      index=int(index)
      index+=1

    #remaining lines
    if cmd.strip().split():
      write_job(cmd, name, outfile)

  write('', 1)

class notracebackException(Exception):
  """ When these exceptions are raised, the traceback is not printed, just a short message """

#######################################################################################################################################
def close_program():
  if sys.exc_info()[0]: #an exception was raised. let's print the information using printerr, shortened if it was a notracebackException
    if issubclass(sys.exc_info()[0], notracebackException):
      printerr( sys.exc_info()[1], 1)
      sys.exit(1)
    elif issubclass(sys.exc_info()[0], SystemExit):      pass
    else:
      printerr('ERROR '+ traceback.format_exc( sys.exc_info()[2]) , 1)   #printing exception in usual format
      sys.exit(2)

if __name__ == "__main__":
  try:
    main()
    close_program()
  except Exception:
    close_program()
