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

=head1 NAME
 
 Retrive OOzie Stats
 This will provide the output in ine format as mentioned below:
 ExternalID-YYYY-MM-DD-HH:<Time Taken for the Job to Complete>:<Created TS>

=head1 SYNOPSIS

 Usage: get_oozie_stats.pl [options]

 where options can be

   --hostname 	|-h Hostname OOzie
   --port       |-p  OOzie Port number for jobs
   --list	| Gives the list of Actions
   --ts	        | Time take for sub flow
   --oa	        | Output Appender
   --oid	| OOzie Id
   --status     | Report status of an oozie job

 Example:

 ./get_oozie_stats.pl -h <ooziehostname> --port 11000 --oid 0000443-120507085729249-oozie-oozi-W   --list
  recordsize
  user-workflow
  falcon-succeeded-messaging
  user-jms-messaging

 ./get_oozie_stats.pl -h <ooziehostname>  --port 11000 --oid 0000443-120507085729249-oozie-oozi-W
  0000443-120507085729249-oozie-oozi-W-2012-05-06-01:1528:1336428818

 ./get_oozie_stats.pl -h <ooziehostname> --port 11000 --oid 0000443-120507085729249-oozie-oozi-W --oa something
  something-2012-05-06-01:1528:1336428818

=head1 AUTHOR

 Hemant Burman <hemant.burman@inmobi.com>
 Kiran Praneeth <kiran.praneeth@gmail.com>

=cut

GetOptions ("hostname|h=s" => \$hostname,
            "port|p=s"      => \$port,
            "status"      => \$status,
            "oid=s"      => \$oid,
            "list+"       => \$list,
            "ts=s"       => \$time_sub,
            "oa=s"       => \$output_appender,
            "help"      => \$help);

$time_sub ||= "total" if !$status;

pod2usage( -exitval => 0 ) if ( $help );
pod2usage( -exitval => 0, -msg => "OOzie Hostname and Port with oozie id is mandatory" ) if ( !defined $hostname || !defined $port || !$oid);

$json = JSON->new->allow_nonref;

my $json_output = `curl -s http://$hostname:$port/oozie/v1/job/$oid?show=info`;

$perl_scalar = from_json( $json_output, { utf8  => 1 } );

sub list_subworkflow {
	$sub_act = $perl_scalar->{'actions'};
	foreach $sa (@$sub_act) {
		print "$sa->{'name'}\n";
	}
}

sub flow_time {
	my $subworkflow_name = shift;
	if ( $time_sub !~ /total/ ) {
		$sub_act = $perl_scalar->{'actions'};
		foreach $sa (@$sub_act) {
			if ( $sa->{'name'} =~ /$subworkflow_name/ ) {
				$start_seconds = parsedate("$sa->{'startTime'}");
				$end_seconds = parsedate("$sa->{'endTime'}");
				print "\n Start Time : $start_seconds End Tme $end_seconds\n";
				print $end_seconds-$start_seconds;
			}
		}
	} else {
		$start_seconds = parsedate("$perl_scalar->{'startTime'}");
		$end_seconds = parsedate("$perl_scalar->{'endTime'}");
		$created_time = parsedate("$perl_scalar->{'createdTime'})");
		$external_id = $perl_scalar->{'externalId'};
		$output_appender ||= $oid;
		#download-summary/DEFAULT/2012-05-06T01:40Z
		if ( $external_id =~ /.*\/(\d{4}-\d{2}-\d{2})T(\d{2})/ ) {
			$tmp_date="$1-$2";
		}
			printf "%s-%s:%s:%d", $output_appender,$tmp_date,$end_seconds-$start_seconds,$created_time;
	}
}

sub get_status {
       # action keys : retries externalId externalStatus status trackerUri toString errorCode endTime startTime id consoleUrl transition name data errorMessage conf cred type
       $act_ref = $perl_scalar->{'actions'};
       if ($perl_scalar->{'status'} eq "WAITING") {
          print Dumper($perl_scalar);
       } elsif ($perl_scalar->{'status'} eq "FAILED" || $perl_scalar->{'status'} eq "KILLED" || $perl_scalar->{'status'} eq "SUSPENDED") {
          foreach my $action(@$act_ref) {
              next if ($action->{'status'} eq "OK");
              push (@action_status, "$action->{'name'}:$action->{'status'}:$action->{'errorCode'}:$action->{'errorMessage'}\n");
          }
          print "JOB state is  $perl_scalar->{'status'} with action states:\n@action_status";
       } elsif ($perl_scalar->{'status'} eq "RUNNING") {
          foreach my $action(@$act_ref) { 
              if ($action->{'status'} eq "RUNNING") {
                  my $action_starttime = parsedate($action->{'startTime'});
                  my $system_time = localtime();
                  $system_time = parsedate($system_time);
                  my $total_seconds = $system_time-$action_starttime;
                  print "JOB is right now running action $action->{'name'} from last $total_seconds seconds\n";
              } 
          }
       } elsif ($perl_scalar->{'status'} eq "SUCCEEDED") {
          foreach my $action(@$act_ref) { 
              my @action_keys = keys %$action;
              push (@action_names, $action->{'name'});
          }
          print "JOB SUCCEEDED and completed actions @action_names\n";
       } else {
          foreach my $action(@$act_ref) {
              next if ($action->{'status'} eq "OK");
              push (@action_status, "$action->{'name'}:$action->{'status'}\n");
          }
          print "JOB in UNKNOWN STATE $perl_scalar->{'status'} and action @action_status\n";
       }
          
}          

list_subworkflow() if ( $list );
flow_time($time_sub) if ( $time_sub );
get_status() if ($status);
