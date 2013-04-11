#!/usr/bin/perl
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

use JSON;
use Data::Dumper;
use Date::Manip;
use Time::ParseDate;
use Getopt::Long;
use Pod::Usage;
use POSIX;


=head1 NAME
 
 Retrive stats for given process and JOBID

=head1 SYNOPSIS

 Usage: falcon_job_status.pl [options]

 where options can be

   --falcon_host |-ivh Hostname where Falcon Server is running
   --falcon_port |-ivp Port on Falcon Server is running
   --oozie_host |-ozh Hostname where Oozie is running
   --oozie_port |-ozp Port on Oozie is running
   --external_id|-eid external ID of your Job
   --deltatime  |-dt This is the time in the past for which you want to look for you Job 
   --processname | -pn Specify the processname you want to get the status for
   --jobid      | -jid Job id for which you want the status for 

 Example:
$ /opt/mkhoj/ops/bin/falcon_job_status.pl -pn <processname> -jid 2012-05-28T06:40Z
JOB SUCCEEDED and completed actions recordsize user-workflow falcon-succeeded-messaging user-jms-messaging falcon-succeeded-log-mover
$

$ /opt/mkhoj/ops/bin/falcon_job_status.pl -pn <processname> -jid 2012-05-28T07:40Z
JOB is right now running action user-workflow from last 717 seconds
$

$ /opt/mkhoj/ops/bin/get_oozie_stats.pl -h <processname> -p 11000 -oid 0004224-120519075902678-oozie-oozi-W -status
JOB SUCCEEDED and completed actions recordsize user-workflow falcon-succeeded-messaging user-jms-messaging falcon-succeeded-log-mover
$

=head1 AUTHOR

 Kiran Praneeth <kiran.praneeth@gmail.com>

=cut

GetOptions ("falcon_host|ivh=s" 	=> \$falcon_host,
            "falcon_port|ivp=i"      	=> \$falcon_port,
            "deltatime|dt=i" 	=> \$dt,
            "oozie_host|ozh=s"       => \$oozie_host,
            "oozie_port|ozp=i"       => \$oozie_port,
            "processname|pn=s"       => \$external_id,
            "jobid|jid=s"       => \$job_id,
            "output_appender|oa=s"       => \$output_appender,
            "output_file|of=s"		=> \$output_file,
            "help"      => \$help);

$falcon_host = "oozie.com" if ! $falcon_host;
$falcon_port = "5800" if ! $falcon_port;
$oozie_host = "oozie.com" if ! $oozie_host;
$oozie_port = "1800" if ! $oozie_port;

pod2usage( -exitval => 0 ) if ( $help );
pod2usage( -exitval => 0, -msg => "falcon server/port, oozie server/port, external id  and job id is Mandatory" ) if ( !defined $falcon_host || !defined $falcon_port || !defined $oozie_host || !defined $oozie_port || !defined $external_id || !defined $job_id);

$dt ||= 60;

$falcon_resp  = `curl -s -H "remote-user: user" "http://$falcon_host:$falcon_port/falcon/api/processinstance/status/$external_id?start=$job_id&end=$job_id"`
        || die "Can't run curl call: $!";

$json = JSON->new->allow_nonref;
$perl_scalar = from_json( $falcon_resp, { utf8  => 1 } );

$falcon_jobs = $perl_scalar->{'instances'};

my $i=0;
my @final_out;
foreach $ij (@$falcon_jobs) {
       $oozie_json=`curl -s -H "remote-user: user" "http://$oozie_host:$oozie_port/oozie/v1/jobs?jobtype=wf&external-id=$external_id/DEFAULT/$ij->{'instance'}"` 
             || die "Failed running oozie status for getting id: $!";
       if ($oozie_json !~ /"id":""/) {
           get_oozie_status();
       } else { 
           get_wait_reason();
       }
}

sub get_oozie_status {
    $oozie_scalar = from_json( $oozie_json, { utf8  => 1 } );
    $op_append = "-oa $output_appender" if ( $output_appender);
    #print "\nRunning /home/gaminik/get_oozie_stats.pl  -h  $oozie_host -p $oozie_port -oid $oozie_scalar->{'id'} -status $op_append\n";
    $oozie_stats = `get_oozie_stats.pl  -h  $oozie_host -p $oozie_port -oid $oozie_scalar->{'id'} -status $op_append `;
    print "Null output from oozie! Check if oozie is down" if !$oozie_stats;
    push (@final_out,$oozie_stats);
}

sub get_wait_reason {
    my $i = 0;
    my $cord_json = `curl -s -H "remote-user: user" "http://$oozie_host:$oozie_port/oozie/v1/jobs?jobtype=coord&filter=name=FALCON_PROCESS_DEFAULT_$external_id"` ||
            die "Failed running curl call: $!";
    $cord_scalar = from_json( $cord_json, { utf8  => 1 } );
    my $cordinator_job_ref = $cord_scalar->{'coordinatorjobs'};
    foreach my $tmp_ref (@$cordinator_job_ref) {
         $cord_job_id = $tmp_ref->{'coordJobId'};
    }
    chomp($cord_job_id);
    my $oozie_stat_json = `curl -s -H "remote-user: user" "http://$oozie_host:$oozie_port/oozie/v1/job/$cord_job_id\@1?show=info"` || 
                 die "Failed running curl call: $!";
    my $oozie_stat_scalar = from_json($oozie_stat_json, { utf8  => 1 } );
    my $oozie_missing_dep = $oozie_stat_scalar->{'missingDependencies'};
    foreach my $dependency (split /#/, $oozie_missing_dep) {
          next if ! $dependency;
          next if $dependency !~ /_SUCCESS$/;
          get_hadoop_status($dependency);
    }
    print "JOB $cord_job_id missing below dependencies:\n";
    print @missing_files;
}

sub get_hadoop_status {
    my $file = @_[0];
    my $success_file_success = `hadoop dfs -ls $file 2>&1 /dev/null` || warn "Can't run hadoop dfs -ls for $file: $!";
    if ($? != 0) {
        push (@missing_files, $file);
    }
}
print "@final_out";
