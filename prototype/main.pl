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

our @g_pipes = ();

our $machine_cnt = 0;

our %g_machine_list = (
    "m00" => {
        "id"  => undef,
        "pid" => undef,
    },
);

our %g_pid_machine_name = ();

# }}}

# {{{ Basci Functions

# {{{ sub get_read_fd

sub get_read_fd
{
    my $machine_id = shift;

    return @g_pipes[$machine_id * 2];
}

# }}}
# {{{ sub get_write_fd
# Description: get write fd by machine_id

sub get_write_fd
{
    my $machine_id = shift;

    return @g_pipes[$machine_id * 2 + 1];
}

#}}}
# {{{ sub send_msg
sub send_msg
{
    my $machine_name = shift;
    my $cmd = shift; # string, length must be less than 16
    my $msg = shift; # serialized by Storable

    if (defined($msg))
    {
        syswrite(&get_write_fd($g_machine_list{$machine_name}{"id"}), pack("A16N", $cmd, length($msg)));
        syswrite(&get_write_fd($g_machine_list{$machine_name}{"id"}), $msg);
    }
    else
    {
        syswrite(&get_write_fd($g_machine_list{$machine_name}{"id"}), pack("A16N", $cmd, 0));
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

# {{{ Setup Env
# In this section, all functions are called by operator / client

# {{{ sub launch_all_machines
sub launch_all_machines
{
    my $cnt = shift;

    die "number should larger than 2" if $cnt < 3;

    for (1...$cnt)
    {
        my ($r, $w);
        pipe($r, $w);
        push(@g_pipes, $r, $w);
    }

    &launch_machine for (1...$cnt);
    select undef, undef, undef, 0.2;
}
#}}}
# {{{ sub launch_machine

sub launch_machine
{
    my $machine_name = sprintf "m%02d", $machine_cnt;

    my $pid = fork;
    if ($pid == 0)
    {
        print color("green") . "[$$] ". $machine_name . " launched" . color("clear"), "\n";
        &worker($machine_cnt);
        exit;
    }
    else
    {
        select undef, undef, undef, 0.1;
        $g_machine_list{$machine_name}{"pid"} = $pid;
        $g_machine_list{$machine_name}{"id"} = $machine_cnt;
        $g_pid_machine_name{$pid} = $machine_name;
        $machine_cnt ++;
    }
}

#}}}
# {{{ sub wait_all_machine
sub wait_all_machine
{
    map { waitpid($_, 0) } keys %g_pid_machine_name;
}
#}}}
# {{{ sub load_machine_info

sub load_machine_info
{
    my $buf = freeze \%g_machine_list;
    map { &send_msg($_, "LOAD_MACHINE_INFO", $buf) } keys %g_machine_list;
}

#}}}

# }}} Setup Env

# {{{ Cluster Instance Functions

# {{{ sub worker
# Description: Instance of one machine, simulate the process on that machine

sub worker
{
    my $machine_id = shift;

    my %kv = (); # simulate local data store on one machine
    my %machine_list = (); # local hash to store all machines list
    my %other_machine_list = (); # local machines list except self

    my @add_events = ();

    while (1)
    {
        my ($cmd, $arg_ref) = &recv_msg( &get_read_fd($machine_id) );

        print color("blue") . "[$$ : $machine_id] $cmd" . color("clear") . "\n";

        if ($cmd eq 'EXIT')
        {
            print color("red") . "[$$ : $machine_id] exit" . color("clear") . "\n";
            return;
        }
        elsif ($cmd eq 'ADD')
        {
            print color("cyan") . "[$$ : $machine_id] ADD ITEM" . color("clear") . "\n";
            $kv{ $arg_ref->{'key'} } = $arg_ref->{'value'};
        }
        elsif ($cmd eq 'ALLKEYS')
        {
            print color("green") . "[$$ : $machine_id] SHOW ALL KEYS" . color("clear") . "\n";
            print Dumper \%kv;
        }
        elsif ($cmd eq 'LOAD_MACHINE_INFO')
        {
            %machine_list = %{$arg_ref};
            %other_machine_list = map { $_ => $machine_list{$_} } grep { $machine_list{$_}{'id'} != $machine_id } keys %machine_list;
        }
    }
}

# }}}

# }}} Cluster Instance Functions

&main();

sub main
{
    &launch_all_machines(3);
    &load_machine_info;

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
