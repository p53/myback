package Term::Shell;

=head1 NAME

    Term::Shell - module for executing shell commands

=head1 SYNOPSIS

    my $shell = Term::Shell->new();
    # executes /usr/bin/ls -al
    $shell->execCmd('cmd' => 'ls -al', 'cmdsNeeded' => ['ls']);
    # executes nohup /usr/bin/ls -al &
    $shell->execCmd('cmd' => 'ls -al', 'cmdsNeeded' => ['ls'], 'bg' => 1, 'detach' => 1);
    # executes ls -al -> note not absolute path
    $shell->exec('cmd' => 'ls -al');

=cut

use Moose;
use namespace::autoclean;
use Data::Dumper;

with 'MooseX::Log::Log4perl';

=head1 METHODS

=over 12

=item C<execCmd>

Method execCmd executes command passed, also finds absolute paths to commands listed in cmdsNeeded parameter

param:

    cmd string - requried parameter, command to execute

    cmdsNeeded array ref - optional parameter, commands which we want to expand to absolute path

    bg boolean - optional parameter, sets if command will be executed in background

    verbose boolean - optional parameter, sets if also command executed will be printed

return:

    $result mixed

=cut

sub execCmd($$) {
	
    my $self = shift;
    my %params = @_;
    my $msg = '';
    my $cmd = $params{'cmd'};
    my $cmdsNeeded = $params{'cmdsNeeded'};

    if(!$params{'cmd'}) {
        $self->log->error("You must supply cmd!");
        die "You must supply cmd!";
    } # if

    my $fullCmd = $self->getAbsPathCmd('cmd' => $cmd, 'cmdsNeeded' => $cmdsNeeded);

    if( defined $params{'detach'} ) {
        $fullCmd = 'nohup ' . $fullCmd; 
    } # if

    if( defined $params{'$bg'} ) {
        $fullCmd = $fullCmd . ' &';
    } # if

    $self->log('debug')->debug("Executing command: $fullCmd\n");

    if( defined $params{'detach'} || defined $params{'bg'} ) {
        system("$fullCmd");
    } else {
        $msg = `$fullCmd 2>&1`;
    } # if

    my $result = $self->analyzeResult('msg' => $msg, 'code' => $?, 'cmd' => $fullCmd);

    return $result;
	
} # end sub execCmd

=item C<exec>

Method exec executes command without absolutizing paths or options to give it to the background or detach

param:

    cmd string

result:

    $result mixed

=cut

sub exec($) {
	
    my $self = shift;
    my %params = @_;
    my $msg = '';
    my $cmd = $params{'cmd'};

    if(!$params{'cmd'}) {
        $self->log->error("You must supply cmd!");
        die "You must supply cmd!";
    } # if

    $self->log('debug')->debug("Executing command: $cmd\n");

    $msg = `$cmd 2>&1`;

    my $result = $self->analyzeResult('msg' => $msg, 'code' => $?, 'cmd' => $cmd);

    return $result;
	
} # end sub exec

=item C<analyzeResult>

Method analyzeResult, handles return codes and stores them in the returned value

param:

    msg string - is the output of command we want to analyze

    code int - is the return code of command

    cmd string - is the command, we were executing

return:

    $result hash ref - looks like this {'returnCode' => 127, 'msg' => 'output of command'}

=cut

sub analyzeResult($$$) {
	
    my $self = shift;
    my %params = @_;
    my $msg = '';
    my $cmd = '';
    $msg = $params{'msg'};
    my $code = $params{'code'};
    $cmd = $params{'cmd'};
    my $result = {};

    if(defined($msg)) {
        chomp($msg);
    } # if

    $self->log('debug')->debug("Checking exit code of shell command");

    if ($code == -1) {
        $msg = "Command $params{'fullCmd'} failed to execute: $!\n";
    } elsif($code == 127) {
        $msg = sprintf("Command $cmd died with signal %d, %s coredump\n", ($code & 127), 'with');
    } elsif($code == 128) {
        $msg = sprintf("Command $cmd died with signal %d, %s coredump\n", ($code & 128), 'without');
    } elsif($code > 0) {
        $msg = sprintf("Command $cmd exited with value %d and message:\n %s\n", $code, $msg);
    } # if

    $self->log('debug')->debug("Return code: ", $code);
    $self->log('debug')->debug("Return message: ", $msg);
    
    $result = {'returnCode' => $code, 'msg' => $msg};

    return $result;
	
} # end sub analyzeResult

=item C<getCmdPath>

Method getCmdPath finds absolute path of the command

param:

    $cmd string

return:

    $cmd string

=cut

sub getCmdPath($) {
	my $self = shift;
	my $cmd = shift;
        
        $self->log('debug')->debug("Getting command path for $cmd");
        
	$cmd = `which $cmd`;
	chomp($cmd);
        
        $self->log('debug')->debug("Full path of command $cmd is: ", $cmd);
        
	return $cmd;
} # end sub getCmdPath

=item C<getAbsPathCmd>

Method getAbsPathCmd replaces all occurences of commands provided in cmdsNeeded parameter with absolute path to them

param:

    cmdsNeeded array ref

    cmd string

return:

    cmdToAbsolutize string

=cut

sub getAbsPathCmd($$) {

	my $self = shift;
	my %params = @_;
	my $cmdsNeeded = $params{'cmdsNeeded'};
	my $cmdToAbsolutize = $params{'cmd'};
	
        $self->log('debug')->debug("Getting absolute command paths");
        
	if(@$cmdsNeeded) {
		foreach my $cmdToAbs(@$cmdsNeeded) {
			if($cmdToAbs && (ref($cmdToAbs) ne 'HASH')) {
				my $cmdAbs = $self->getCmdPath($cmdToAbs);
				$cmdToAbsolutize =~ s/\b$cmdToAbs\b/$cmdAbs/g;
			} # if
		} # foreach
	} # if
	
        $self->log('debug')->debug("Dumping absolute command paths: ", sub{ Dumper($cmdToAbsolutize) });
        
	return $cmdToAbsolutize;
	
} # end sub getAbsPathCmd

=item C<warning>

Method warning serves for creating warning if return code isnt 1 - this uses module internal return codes, not shell!

params:

    $result int - return code of command

=cut

sub warning($) {
	
    my $self = shift;
    my $result = shift;
    my $class = ref $self;

    if($result->{'returnCode'} != 0) {
        $self->log->warn("$class " . $result->{'msg'});
        warn("WARNING: $class " . $result->{'msg'});
    } # if
	
} # end sub warning

=item C<fatal>

Method fatal serves for dieing if return code of command isnt 1 - this uses module internal return codes, not shell!

params:

    $result int - return code of command

=back

=cut

sub fatal() {
	
    my $self = shift;
    my $result = shift;
    my $class = ref $self;

    if($result->{'returnCode'} != 0) {
        $self->log->error("ERROR: $class " . $result->{'msg'});
        die "DIED: $class " . $result->{'msg'};
    } # if
	
} # end sub fatal

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut

1;