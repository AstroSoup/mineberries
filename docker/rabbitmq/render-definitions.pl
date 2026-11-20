#!/usr/bin/perl
use strict;
use warnings;

local $/;
my $template = <STDIN>;
die "definitions template is empty\n" unless defined $template && length $template;

$template =~ s{\$\{([A-Z0-9_]+)\}}{
    my $name = $1;
    die "missing environment variable $name\n"
        unless defined $ENV{$name} && length $ENV{$name};
    json_escape($ENV{$name});
}ge;

print $template;

sub json_escape {
    my ($value) = @_;
    $value =~ s/\\/\\\\/g;
    $value =~ s/"/\\"/g;
    $value =~ s/\n/\\n/g;
    $value =~ s/\r/\\r/g;
    $value =~ s/\t/\\t/g;
    $value =~ s/([\x00-\x1f])/sprintf('\\u%04x', ord($1))/ge;
    return $value;
}
