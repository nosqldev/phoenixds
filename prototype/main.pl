#!/usr/bin/perl -w

# © Copyright 2015 jingmi. All Rights Reserved.
#
# +-----------------------------------------------------------------------+
# | prototype of phoenix data store                                       |
# +-----------------------------------------------------------------------+
# | Author: jingmi@gmail.com                                              |
# +-----------------------------------------------------------------------+
# | Created: 2015-04-18 14:34                                             |
# +-----------------------------------------------------------------------+

use strict;
use Data::Dumper;
use Storable qw(freeze thaw);
use Term::ANSIColor;
use POSIX ":sys_wait_h";

# {{{ global var

our $machine_cnt = 0;

our %machine_list = (
    "m00" => {
        "read_fd"  => undef,
        "write_fd" => undef,
        "pid"      => undef,
    },
);

our %pid_machine_name = ();

# }}}
# {{{ Basci Functions

# {{{ sub send_msg
sub send_msg
{
    my $machine_id = shift;
    my $cmd = shift; # string, length must be less than 16
    my $msg = shift; # serialized by Storable

    if (defined($msg))
    {
        syswrite($machine_list{$machine_id}{"write_fd"}, pack("A16N", $cmd, length($msg)));
        syswrite($machine_list{$machine_id}{"write_fd"}, $msg);
    }
    else
    {
        syswrite($machine_list{$machine_id}{"write_fd"}, pack("A16N", $cmd, 0));
    }
}
#}}}
# {{{ sub recv_msg
sub recv_msg
{
    my $pipe_id = shift;
    my ($buf, $cmd, $msg_len, $msg);

    sysread($pipe_id, $buf, 20);
    ($cmd, $msg_len) = unpack("A16N", $buf);
    if ($msg_len != 0)
    {
        sysread($pipe_id, $msg, $msg_len);
        my %arg = %{ thaw($msg) };
        return $cmd, \%arg;
    }
    else
    {
        return $cmd, undef;
    }
}
#}}}

# }}} Basic Functions
# {{{ Setup env

# {{{ sub launch_machine
sub launch_machine
{
    my $machine_name = sprintf "m%02d", $machine_cnt;
    my ($rfd, $wfd);

    pipe($rfd, $wfd);

    my $pid = fork;
    if ($pid == 0)
    {
        print color("green") . "[$$] ". $machine_name . " launched" . color("clear"), "\n";
        &worker($rfd);
        exit;
    }
    else
    {
        select undef, undef, undef, 0.1;
        $machine_list{$machine_name}{"read_fd"} = $rfd;
        $machine_list{$machine_name}{"write_fd"} = $wfd;
        $machine_list{$machine_name}{"pid"} = $pid;
        $pid_machine_name{$pid} = $machine_name;
        $machine_cnt ++;
    }
}
#}}}
# {{{ sub wait_all_machine
sub wait_all_machine
{
    map { waitpid($_, 0) } keys %pid_machine_name;
}
#}}}

# }}} Setup env
# {{{ Cluster Instance Functions

# {{{ sub worker
# Description: Instance of one machine, simulate the process on that machine

sub worker
{
    my $local_rfd = shift;

    my %kv = (); # simulate local data store on one machine

    while (1)
    {
        my ($cmd, $arg_ref) = &recv_msg($local_rfd);

        print color("blue") . "[$$] $cmd" . color("clear") . "\n";

        if ($cmd eq 'EXIT')
        {
            #print color("red") . "$$ exit" . color("clear") . "\n";
            return;
        }
        elsif ($cmd eq 'ADD')
        {
            #print color("red") . "$$ add keys: $arg_ref->{'key'}" . color("clear") . "\n";
            $kv{ $arg_ref->{'key'} } = $arg_ref->{'value'};
        }
        elsif ($cmd eq 'ALLKEYS')
        {
            print Dumper \%kv;
        }
    }
}

# }}}

# }}} Cluster Instance Functions

&main();

sub main
{
#    my ($rfd, $wfd);
#    pipe($rfd, $wfd);
#
#    $machine_list{"m00"}{"read_fd"} = $rfd;
#    $machine_list{"m00"}{"write_fd"} = $wfd;
#
#    print Dumper(\%machine_list);
#
#    my %hash = ("1"=>2);
#    my $msg = freeze \%hash;
#    &send_msg("m00", "OK", $msg);
#    my ($cmd, $hash_ref) = &recv_msg($rfd);
#
#    print $cmd, "\n";
#    print Dumper($hash_ref);

    &launch_machine;
    &launch_machine;
    &launch_machine;

    #print Dumper(\%machine_list);
    #print Dumper(\%pid_machine_name);

    my $arg = freeze {'key'=>'city', 'value'=>'beijing'};

    &send_msg("m00", "ADD", $arg);
    &send_msg("m00", "ALLKEYS", $arg);

    &send_msg("m00", "EXIT", undef);
    &send_msg("m01", "EXIT", undef);
    &send_msg("m02", "EXIT", undef);

    &wait_all_machine;
}

__END__
# vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker:
