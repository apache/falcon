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
use Time::Local;
use Getopt::Long;
use Pod::Usage;
use strict;

our ($json, $result, $threshold, $process, $start, $ooziehost, $end, $hour, $date, $nexthour, $instance_id, $jobthreshold);
our ($help, $httphost, $query, $query_output, $httphost_url, @failed, @running, @succeeded, @waiting, @unknown);

$json = JSON->new->allow_nonref;

$result = GetOptions ("threshold=i" => \$threshold,
                      "jthreshold=i" => \$jobthreshold,
                      "proc=s" => \$process,
                      "start=s" => \$start,
                      "ooh=s" => \$ooziehost,
                      "end=s" => \$end,
                      "help" => \$help);

&pod2usage() if $help;

die "Process (--proc) must be specified, Run --help\n"  if !$process;

if (!$start && !$end) {
    $hour = `date +%H`;
    chomp($hour);
    $date = `date +%F`;
    chomp($date);
    $nexthour = $hour - 3;
    $end = "$date"."T$hour:00Z";
    $start= "$date"."T$nexthour:00Z";
}

if (!$threshold) {
    $threshold = 120;
}

if (!$jobthreshold) {
    $jobthreshold = 40;
}


$threshold = $threshold * 60;
$jobthreshold = $jobthreshold * 60;

$ooziehost = "oozie.com:5999" if !$ooziehost;
$httphost_url = "http://$ooziehost/falcon/api/processinstance/status/$process?start=$start&end=$end";
$query = "curl -s -H'remote-user: user'"." \"$httphost_url\"";
$query_output = `$query` || die "Curl call failed: $!";

if ($query_output eq "") {
    print "Null output. Check if Falcon is down or jobs are hung from long time\n";
    exit 2;
}

my $instance = from_json( $query_output, { utf8  => 1 } );

my $instances_ref = $instance->{instances};

foreach (@$instances_ref) {
    my $instance_ref = $_;
    my $alert_string = "";
    my $instance_id = $instance_ref->{instance};
    if ($instance_ref->{status} eq "FAILED" || $instance_ref->{status} eq "KILLED" || $instance_ref->{status} eq "SUSPENDED") {
        my $actions_ref = $instance_ref->{actions};
        foreach(@$actions_ref) {
            my $action_ref = $_;
            if ($action_ref->{status} eq "FAILED") {
               $alert_string = "CRITICAL: JOB $instance_id failed at action $action_ref->{action}\n";
            }  elsif ($action_ref->{status} eq "KILLED") {
               $alert_string = "CRITICAL: JOB $instance_id killed at action $action_ref->{action}\n";
            } elsif ($action_ref->{status} eq "SUSPENDED") {
               $alert_string = "CRITICAL: JOB $instance_id is in suspended state\n";
            }
            push (@failed, $alert_string);
        }
    } elsif ($instance_ref->{status} eq "WAITING") {
        #&get_process_lag($instance_id);
        $instance_id =~ s/(-|[A-Z])/:/g;
        my ($year, $mon, $day, $hour, $min) = split /:/, $instance_id;
        $mon = $mon -1;
        my $instance_epoch =  timelocal(00, $min, $hour, $day, $mon, $year);
        my $system_epoch = time();
        my $job_lag = int($system_epoch - $instance_epoch);
        if ($job_lag > $threshold) {
            $alert_string = "CRITICAL: JOB $instance_id is in waiting state for more then given thresholds;";
            push (@waiting, $alert_string);
        } 
    } elsif ($instance_ref->{status} eq "RUNNING") {
        #&get_process_lag($instance_id);
        my $instance_id_raw = $instance_id;
        $instance_id =~ s/(-|[A-Z])/:/g;
        my ($year, $mon, $day, $hour, $min) = split /:/, $instance_id;
        $mon = $mon -1;
        my $instance_epoch =  timelocal(00, $min, $hour, $day, $mon, $year);
        my $system_epoch = time();
        my $job_lag = int($system_epoch - $instance_epoch);
        if ($job_lag > $threshold) {
            $alert_string = "CRITICAL: JOB $instance_id is in running state for more then given thresholds;";
            push (@running, $alert_string);
        } 
        # 2012-06-05T06:40Z
        my @falcon_output = `falcon_job_status.pl --proc $process -jid $instance_id_raw`;
        if ($? != 0 ) {
           $alert_string = "CRITICAL: Failed running falcon_job_status.pl --proc $process -jid $instance_id";
           push (@running, $alert_string);
        }
        next if !$falcon_output[0];
        my $jobruntime;
        if ($falcon_output[0] =~ /last(.*)seconds/ ) {
            $jobruntime = $1;
        }
        $jobruntime =~ s/\s+//g;
        if ($jobruntime  > $jobthreshold) {
            $alert_string = "CRITICAL: JOB $instance_id is in running state from last $jobruntime but threshold is $jobthreshold\n";
            push (@running, $alert_string);
        }

    } elsif ($instance_ref->{status} eq "SUCCEEDED") {
        push (@succeeded, $instance_id);
    } else {
        push (@unknown, $instance_id);
    }
}
&nagios_alert();

sub get_process_lag {
        $instance_id =~ s/(-|[A-Z])/:/g;
        my ($year, $mon, $day, $hour, $min) = split /:/, $instance_id;
        $mon = $mon -1;
        my $instance_epoch =  timelocal(00, $min, $hour, $day, $mon, $year);
        my $system_epoch = time();
        my $job_lag = int($system_epoch - $instance_epoch);
        return 1 if ($job_lag > $threshold);
}

sub nagios_alert {
   if (@failed || @waiting || @running) {
       print @failed if @failed;
       print @waiting if @waiting;
       print @running if @running;
       exit 2;
   } elsif (@unknown) {
       print @unknown;
       exit 1;
   } elsif (@succeeded) {
       print "Succeded:@succeeded";
       exit 0;
   } else {
       print "UNKNOWN STATE\n";
       exit 1;
   }
}

__END__

=head1 NAME
 
Check the status of an Falcon job in a given window and report it to Nagios

=head1 SYNOPSIS

 Usage: check_falcon_job.pl [options]

 where options can be

   --threshold	Threshold in minutes. Jobs in waiting or suspended state will be reported if (systemtime - jobid time) exceeds thsi threshold
   --jthreshold	Threshold in minutes. Jobs in waiting or suspended state will be reported if (systemtime - jobid time) exceeds thsi threshold
   --proc	Specify the process name
   --start	Startime for the sample window
   --ooh	To specify a oozie host. By default it is oozie.red.ua2.inmobi.com
   --end	Endtime for the sample window

 Example:

$ perl check_falcon_job.pl --proc download-summary
Succeded:2012-05-28T05:40Z 2012-05-28T06:40Z
gaminik@proc2000:/opt/mkhoj/ops/lib/nrpe

$ perl check_falcon_job.pl --proc <processname> --start 2012-05-28T01:40Z --end 2012-05-28T06:40Z
Succeded:2012-05-28T01:40Z 2012-05-28T02:40Z 2012-05-28T03:40Z 2012-05-28T04:40Z 2012-05-28T05:40Z 2012-05-28T06:40Z
$ 

$ perl check_falcon_job.pl --proc <processname> --start 2012-05-28T07:40Z --end 2012-05-28T08:40Z
UNKNOWN STATE
$

$ perl check_falcon_job.pl --proc <processname> --start 2012-05-28T03:40Z --end 2012-05-28T07:40Z --threshold 10
CRITICAL: JOB 2012:05:28:07:40: is in waiting state for more then given thresholds;CRITICAL: JOB 2012:05:28:06:40: is in running state for more then given thresholds;
$ 

$perl check_falcon_job.pl --proc <processname> --start 2012-05-28T03:40Z --end 2012-05-28T07:40Z --threshold 100
Succeded:2012-05-28T03:40Z 2012-05-28T04:40Z 2012-05-28T05:40Z
$

=head1 AUTHOR

 Kiran Praneeth <kiran.praneeth@gmail.com>

=cut

