#!/usr/bin/perl -w
#
# https://github.com/mshinall
#
# mysql style command line script for querying atomics
#

require v5.06.00;

use strict;
use XML::XPath;
use LWP;
use Data::Dumper;
use URI::Escape;
use Encode;
use Term::ReadLine;
use Term::ReadLine::Perl;
use Term::ReadLine::Gnu;
use Getopt::Std;
use Log::Log4perl;

use constant USE_TR => eval("use Term::ReadLine; 1") ? 1 : 0;

my $OPTIONS = {};

my $ua = LWP::UserAgent->new();
$ua->timeout(10000);

my $term = undef;
if(USE_TR) {
    $term = Term::ReadLine->new('Atomics Client');
    $term->ornaments(0);
}

if(scalar(@ARGV) <= 0) {
    stdout("Usage: atomics.pl [-l LOGLEVEL] HOST_URI[:PORT]\n");
    stdout("    LOGLEVEL: one of TRACE, DEBUG, INFO, WARN, ERROR, FATAL. Defaults to INFO.\n");
    exit(1);
}

getopts('l:', $OPTIONS);

my $logLevel = 'INFO';
if(exists($$OPTIONS{'l'})) {
    $$OPTIONS{'l'} =~ /^\s*(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\s*$/i && do {
        $logLevel = uc($1);
        stdout("Log level set to '${logLevel}'.\n");
    }
}

Log::Log4perl->init({
    'log4perl.logger' => "${logLevel}, screen",
    'log4perl.appender.screen' => 'Log::Log4perl::Appender::Screen',
    'log4perl.appender.screen.layout' => 'Log::Log4perl::Layout::PatternLayout',
    'log4perl.appender.screen.layout.ConversionPattern' => '%d %p %l - %m%n',
});
my $logger = Log::Log4perl->get_logger();

my ($host) = @ARGV;

#make sure connection is good
checkConnection();

stdout("Using atomics host: ${host}\n\n");
stdout("Type 'help' for help.\n\n");

#main loop
#local $SIG{TERM} = \&handleSigTerm; # daemon framework stop command uses this
#local $SIG{INT} = \&handleSigInt; # ^C from terminal
#my $cancel = 0;

while(1) {
    $_ = stdin("atomics> ");
    #if(USE_TR) {
    #    $_ = $term->readline("atomics> ");
    #} else {
    #    stdout("atomics> ");
    #    $_ = <STDIN>;
    #}
    chomp($_);
    $_ = trim($_);
    /^exit|quit|\\q$/i && do { last; };
    /^help|\\h$/i      && do { printHelp(); next; };
    (length($_) > 0)   && do { query($_); };
}
stdout("\n");
exit(0);

sub handleSigTerm() {
    $logger->trace("Process Terminated");
}

sub handleSigInt() {
    $logger->trace("Process Interrupted");
}

sub printHelp {
    print <<_END_;
List of commands:

?       (\\?) Synonym for 'help'.    
clear;  (\\c) Clear the query.
exit    (\\q) Exit the client.
go;     (\\g) Send the query to atomics (Not needed if a ';' terminates the query string).
help    (\\h) This text
quit    (\\q) Synonym for 'exit'.

*notes:
-Use the clause "into outfile 'some_filename'" at the end of a query to output results into a file. (Currently only supports output to csv format.)

_END_

}

sub query {
    my ($query) = @_;
    $query = trim($query);
    
    $query =~ /;$/i && do {
        atomics($query);
        return;
    };
        
    while(1) {
        $_ = stdin("      -> ");
        #if(USE_TR) {
        #    $_ = $term->readline("      -> ");
        #} else {
        #    stdout("      -> ");
        #    $_ = <STDIN>;
        #}    
        chomp($_);
        $_ = trim($_);
        #user request to submit query now
        /^go;|\\g$/i     && do { atomics($query . ";"); last; };
        #user request to clear query buffer
        /^clear;|\\c$/i  && do { $query = ""; last; };
        #line ends in semi so submit query now        
        /;$/i            && do { atomics($query . ";"); last; };        
        #input added to query buffer
        (length($_) > 0) && do { $query .= " " . $_; };        
    }
}

sub stdin {
    my ($prompt) = @_;
    my $input = '';
    if(USE_TR) {
        $input = $term->readline($prompt);
    } else {
        stdout($prompt);
        $input = <STDIN>;
    }
    return $input;
}

sub checkConnection {
    my $results = getAtomicsResults("select now() as now;");
    if($results->{'error'} && ($results->{'error'} == 1)) {
        $logger->error("There was a problem connecting to '${host}', please verify the uri and port.");
        exit(1);
    }
}

sub stdout {
    my ($string) = @_;
    print($string);    
}

sub atomics {
    my ($query) = @_;
    my $outfile = "";
    $query = trim($query);
    
    #if query includes outfile clause
    $query =~ /into\soutfile\s'([^']+)'\s*/i && do {
        #grab filename
        $outfile = $1;
        #remove clause from query
        $query =~ s/into\soutfile\s'[^']+'\s*//gi;
    };
        
    my $results = getAtomicsResults($query);
    my $records = $results->{'records'};        
    my $recordCount = $results->{'recordCount'};

    #if($results->{'recordCount'} > 0) {
        printAtomicsHeader($results);    
        if($outfile) {
            outputAtomics($results, $outfile);        
        } else {
            printAtomics($results);
        }
    #}
}

sub getAtomicsResults {
    my ($query) = @_;
    $logger->debug("Querying atomics '${query}' ...");
    $query = URI::Escape::uri_escape($query);
    my $url = "${host}/raw/?version=1&q=${query}";    
    $logger->trace("atomics call: ${url}");
    my $response = $ua->get($url);
    my $content = "";
    my $results = {
        'records' => [],
        'recordCount' => 0,
        'status' => '',
        'numFields' => '',
        'numRecords' => '',
        'QTime' => '',
    };
    if($response->is_success)
    {
        $content = $response->decoded_content;
        if($content && (length($content) > 0)) {
            $results = parseAtomics($content);
            $logger->debug(" Found " . $results->{'recordCount'} . " records.");
        } else {
            $logger->error("Unexpected response from host.");
        }
    }
    else
    {
        $results->{'error'} = 1;
        $results->{'error_code'} = $response->code;
        $results->{'error_status'} = $response->status_line;            
        $results->{'error_content'} = '';
        #if($response->code != 500) {
        #    $content = $response->decoded_content;
        #}
        if($content && (length($content) > 0)) {
            $results->{'error_content'} = $content;            
            $logger->error($response->status_line . ": " . $content);
        } else {
            $logger->error("Unexpected response from host: " . $response->status_line);                    
        }
    }
    return $results;
}

sub printAtomicsHeader {
    my ($results) = @_;
    
    my $status = $results->{'status'};
    my $numFields = $results->{'numFields'};
    my $numRecords = $results->{'numRecords'};
    my $qTime = $results->{'QTime'};
    
    my $statusLine = "Status: ${status}";
    my $numFieldsLine = "Number of Fields: ${numFields}";
    my $numRecordsLine = "Number of Records: ${numRecords}";
    my $qTimeLine = "Query Time: ${qTime} (ms)";

    my $size = length($statusLine);
    if(length($numFieldsLine) > $size) { $size = length($numFieldsLine); }
    if(length($numRecordsLine) > $size) { $size = length($numRecordsLine); }
    if(length($qTimeLine) > $size) { $size = length($qTimeLine); }
    
    my $border = "+-" . ("-" x $size) . "-+";
    stdout($border . "\n");
    stdout("| " . $statusLine . (" " x ($size - length($statusLine))) . " |\n");
    stdout("| " . $numFieldsLine . (" " x ($size - length($numFieldsLine))) . " |\n");
    stdout("| " . $numRecordsLine . (" " x ($size - length($numRecordsLine))) . " |\n");
    stdout("| " . $qTimeLine . (" " x ($size - length($qTimeLine))) . " |\n"); 
    stdout($border . "\n");
    stdout("\n");
}

sub outputAtomics {
    my ($results, $outfile) = @_;
    my $out;
    #return out early if there are no records to display
    if(scalar(@{$results->{'records'}}) <= 0) {
        return;
    }

    #if(-f $outfile) {
    #    stdout("File '${outfile}' already exists, please choose a different filename.\n");
    #    return;
    #} else {
        open($out, ">${outfile}") || do {
            stdout("Unable to open file '${outfile}' for writing. Please try again. (${!})\n");
            return;
        };
    #}
        
    print($out "\"" . join("\",\"", @{$results->{'columns'}}) . "\"\n");
    
    stdout("Writing records...\n");
    foreach my $row (@{$results->{'records'}}) {
        #stdout(".");    
        my $i = 0;
        foreach my $col (@{$results->{'columns'}}) {
            if($i++ > 0) {
                print($out ",");            
            }
            my $value = $row->{$col}->{'value'};
            $value =~ s/"/'/gi;
            my $type = $row->{$col}->{'type'};            
            if($type =~ /integer|date/i) {
                print($out $value);
            } else {
                print($out "\"" . $value . "\"");
            }
        }
        print($out "\n");
    }
    stdout("\n\n");    
    print($out "\n");
    close($out);
    stdout("Results written to '${outfile}'.\n");
}

sub printAtomics {
    my ($results) = @_;

    #return out early if there are no records to display
    if(scalar(@{$results->{'records'}}) <= 0) {
        return;
    }

    my $numRecords = $results->{'numRecords'};
    my $qTime = $results->{'QTime'};
    
    my $line = "|";
    my $rowSep = "+";
    foreach my $col (@{$results->{'columns'}}) {
        my $size = $results->{'colSize'}->{$col};
        my $pad = $size - length($col);
        $line   .= " " . $col . (" " x $pad) . " |";
        $rowSep .= "-" . ("-" x $size) . "-+";
    }
    
    #$rowSep = "+" . ("-" x (length($line) - 2)) . "|";
    stdout($rowSep . "\n");
    stdout($line . "\n");    
    stdout($rowSep . "\n");        
    
    foreach my $row (@{$results->{'records'}}) {
        $line = "|";
        foreach my $col (@{$results->{'columns'}}) {
            my $value = $row->{$col}->{'value'};
            my $type = $row->{$col}->{'type'};
            my $size = $results->{'colSize'}->{$col};
            my $pad = $size - length($value);
            if($type =~ /integer|date/i) {
                $line .= " " . (" " x $pad) . $value . " |";                        
            } else {
                $line .= " " . $value . (" " x $pad) . " |";                        
            }
        }
        stdout($line . "\n");
        #stdout($rowSep . "\n");
    }
    stdout($rowSep . "\n");    
    stdout("${numRecords} rows in set (${qTime} ms)\n");
    stdout("\n");
    
}

sub parseAtomics {
    my ($content) = @_;
    my $xp = XML::XPath->new(xml => $content);
    my $obj = {};
    $obj->{'status'} = "";
    $obj->{'numFields'} = "";
    $obj->{'numRecords'} = "";
    $obj->{'QTime'} = "";
    $obj->{'recordCount'} = 0;
    my $header = $xp->find('/response/responseHeader')->get_node(1);    
    if($header) {
        $obj->{'status'} = toUtf8($header->find('status')->string_value);
        $obj->{'numFields'} = toUtf8($header->find('numFields')->string_value);
        $obj->{'numRecords'} = toUtf8($header->find('numRecords')->string_value);
        $obj->{'QTime'} = toUtf8($header->find('QTime')->string_value);
    }
    my $records = $xp->find('/response/responseBody/record');
    
    #set up columns
    $obj->{'columns'} = [];
    $obj->{'colSize'} = {};
    $obj->{'records'} = [];
    my $firstRec = $records->get_node(1);
    if($firstRec) {
        my $fields = $firstRec->find('field');
        foreach my $field ($fields->get_nodelist()) {
            my $name = toUtf8($field->find('name')->string_value);
            $obj->{'colSize'}->{$name} = length($name);
            push(@{$obj->{'columns'}}, $name);
        }
    }
    
    foreach my $record ($records->get_nodelist()) {
        my $fields = $record->find('field');
        my $recObj = {};
        foreach my $field ($fields->get_nodelist()) {
            my $name = toUtf8($field->find('name')->string_value);
            my $value = toUtf8($field->find('value')->string_value);
            my $type = $field->getAttribute('type');
            if(length($value) > $obj->{'colSize'}->{$name}) {
                $obj->{'colSize'}->{$name} = length($value);
            }
            $recObj->{$name} = {'value' => $value, 'type' => $type};
        }
        push(@{$obj->{'records'}}, $recObj);
    }
    $obj->{'recordCount'} = scalar(@{$obj->{'records'}});
    $logger->trace(Dumper($obj) . "\n");        
    return $obj;
}

sub trim {
    my ($string) = @_;
    #trim leading and trailing spaces
    $string =~ s/^\s+//gi;
    $string =~ s/\s+$//gi;
    return $string;
}

sub toUtf8 {
    my ($string) = @_;
    if($string && (length($string) > 0)) {
        $string = Encode::encode("UTF-8", $string, Encode::FB_HTMLCREF);
    }
    return $string;
}

1;
