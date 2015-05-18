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

our $node_cnt = 0;

our %g_node_list = (
    "m00" => {
        "id"  => undef,
        "pid" => undef,
    },
);

our %g_pid_node_name = ();

# }}}

# {{{ Basci Functions

# {{{ sub get_read_fd

sub get_read_fd
{
    my $node_id = shift;

    return @g_pipes[$node_id * 2];
}

# }}}
# {{{ sub get_write_fd
# Description: get write fd by node_id

sub get_write_fd
{
    my $node_id = shift;

    return @g_pipes[$node_id * 2 + 1];
}

#}}}
# {{{ sub send_msg

sub send_msg
{
    my $node_name = shift;
    my $cmd = shift; # string, length must be less than 16
    my $msg = shift; # serialized by Storable

    if (defined($msg))
    {
        syswrite(&get_write_fd($g_node_list{$node_name}{"id"}), pack("A16N", $cmd, length($msg)));
        syswrite(&get_write_fd($g_node_list{$node_name}{"id"}), $msg);
    }
    else
    {
        syswrite(&get_write_fd($g_node_list{$node_name}{"id"}), pack("A16N", $cmd, 0));
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

# {{{ sub launch_all_nodes
sub launch_all_nodes
{
    my $cnt = shift;

    die "number should larger than 2" if $cnt < 3;

    for (1...$cnt)
    {
        my ($r, $w);
        pipe($r, $w);
        push(@g_pipes, $r, $w);
    }

    &launch_node for (1...$cnt);
}
#}}}
# {{{ sub launch_node

sub launch_node
{
    my $node_name = sprintf "m%02d", $node_cnt;

    my $pid = fork;
    if ($pid == 0)
    {
        print color("green") . "[$$] ". $node_name . " launched" . color("clear"), "\n";
        &worker($node_cnt);
        exit;
    }
    else
    {
        select undef, undef, undef, 0.1;
        $g_node_list{$node_name}{"pid"} = $pid;
        $g_node_list{$node_name}{"id"} = $node_cnt;
        $g_pid_node_name{$pid} = $node_name;
        $node_cnt ++;
    }
}

#}}}
# {{{ sub wait_all_node
sub wait_all_node
{
    map { waitpid($_, 0) } keys %g_pid_node_name;
}
#}}}
# {{{ sub load_node_info

sub load_node_info
{
    my $buf = freeze \%g_node_list;
    map { &send_msg($_, "LOAD_CONF", $buf) } keys %g_node_list;
    select undef, undef, undef, 0.2;
}

#}}}

# }}} Setup Env

# {{{ Cluster Instance Functions

# {{{ sub worker
# Description: Instance of one node, simulate the process on that node

sub worker
{
    my $node_id = shift;

    my %kv = (); # simulate local data store on one node
    my %node_list = (); # local hash to store all nodes list
    my %other_node_list = (); # local nodes list except self

    while (1)
    {
        my ($cmd, $arg_ref) = &recv_msg( &get_read_fd($node_id) );

        print color("blue") . "[$$ : $node_id] $cmd" . color("clear") . "\n";

        if ($cmd eq 'EXIT')
        {
            #print color("red") . "[$$ : $node_id] exit" . color("clear") . "\n";
            return;
        }
        elsif ($cmd eq 'ADD')
        {
            print color("cyan") . "[$$ : $node_id] ADD ITEM" . color("clear") . "\n";
            my $event = &writeahead_event_add($arg_ref->{'key'}, $arg_ref->{'value'}, \%other_node_list);
            &exec_transaction($event);
        }
        elsif ($cmd eq 'ALLKEYS')
        {
            print color("green") . "[$$ : $node_id] KEYS CNT: " . length(keys %kv) . color("clear") . "\n";
            #print Dumper \%kv;
        }
        elsif ($cmd eq 'LOAD_CONF')
        {
            %node_list = %{$arg_ref};
            %other_node_list = map { $_ => $node_list{$_} } grep { $node_list{$_}{'id'} != $node_id } keys %node_list;
        }
    }
}

sub writeahead_event_add
{
    my $key = shift;
    my $value = shift;
    my $other_node_list = shift;

    my @other_node_list = sort {$a cmp $b} keys %{$other_node_list};

    my %event_item = (
        'action' => 'add',
        'key' => $key,
        'value' => $value,
        'other_node_list' => \@other_node_list,
    );

    return \%event_item;
}

sub exec_transaction
{
    my $event = shift;

    &prepare_transaction($event);
    #&commit_transanction;
}

sub prepare_transaction
{
    my $event = shift;
    foreach my $node_name (@{$event->{'other_node_list'}})
    {
    }
}

# }}}

# }}} Cluster Instance Functions

&main();

sub main
{
    &launch_all_nodes(3);
    &load_node_info;

    my $arg = freeze {'key'=>'city', 'value'=>'beijing'};

    &send_msg("m00", "ADD", $arg);
    &send_msg("m00", "ALLKEYS", $arg);

    &send_msg("m00", "EXIT", undef);
    &send_msg("m01", "EXIT", undef);
    &send_msg("m02", "EXIT", undef);

    &wait_all_node;
}

__END__
# vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker:
