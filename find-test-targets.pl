#!/usr/bin/env perl
use v5.20;
use strict;
use warnings;

use List::MoreUtils qw/uniq/;
use MongoDB;
use Parse::CPAN::Packages::Fast;
use boolean;

my $mc   = MongoDB::MongoClient->new;
my $coll = $mc->ns("cpan.meta");

#--------------------------------------------------------------------------#
# find Test:: modules
#--------------------------------------------------------------------------#

my $p =
  Parse::CPAN::Packages::Fast->new("/srv/cpan/modules/02packages.details.txt.gz");

my $pkg_to_dist = $p->{pkg_to_dist};

my @test_modules =
  map { s{^./../}{}r }
  map { $pkg_to_dist->{$_} } grep { /^Test::/ } keys %$pkg_to_dist;

my %latest =
  map { $_ => undef }
  map { s{^./../}{}r } map { $_->pathname } $p->latest_distributions;

#--------------------------------------------------------------------------#
# find modules that rely on a Test:: module other than Test::More
# or Test::Simple
#--------------------------------------------------------------------------#

my @pipeline = (
    { '$unwind'  => '$flat_prereqs' },
    { '$match'   => { flat_prereqs => { '$regex' => '^Test::' } } },
    { '$match'   => { flat_prereqs => { '$nin' => [ 'Test::More', 'Test::Simple' ] } } },
    { '$project' => { _id => 0, distfile => '$_id', requires => '$flat_prereqs' } },
    {
        '$group' => {
            _id     => '$distfile',
            count   => { '$sum' => 1 },
            modules => { '$addToSet' => '$requires' }
        }
    },
    { '$sort' => { _id => 1 } },
);

my $agg = $coll->aggregate( \@pipeline, { cursor => 1 } );

my @dependents = map { $_->{_id} } $agg->all;

#--------------------------------------------------------------------------#
# modules that have Test::Builder::Level
#--------------------------------------------------------------------------#

# put in HERE doc from grep.cpan.me
my @test_builder_level = split " ", <<HERE;
AGENT/Makefile-DOM-0.008.tar.gz
AGENT/Makefile-Parser-0.216.tar.gz
ALEXMV/Net-IMAP-Server-1.38.tar.gz
AMBS/Math-GSL-0.35.tar.gz
AYANOKOUZ/WebService-Simple-Yahoo-JP-API-0.11.tar.gz
BEPPU/Squatting-On-PSGI-0.06.tar.gz
BINGOS/ExtUtils-MakeMaker-7.04.tar.gz
BTROTT/Data-ConveyorBelt-0.02.tar.gz
BTROTT/Feed-Find-0.07.tar.gz
BTROTT/XML-FOAF-0.04.tar.gz
CHIBA/Plack-Middleware-RefererCheck-0.03.tar.gz
COOK/Device-SerialPort-1.04.tar.gz
CORION/Test-HTML-Content-0.09.tar.gz
DAGOLDEN/CPAN-Reporter-1.2014.tar.gz
DANJOU/AnyEvent-DBI-Abstract-Limit-0.02.tar.gz
DCONWAY/Test-Effects-0.001005.tar.gz
DOY/KiokuDB-0.57.tar.gz
DROLSKY/Fey-Test-0.10.tar.gz
DROLSKY/Test-DependentModules-0.20.tar.gz
DWHEELER/App-Sqitch-0.9991.tar.gz
EWILHELM/dotReader-v0.11.2.tar.gz
EXODIST/Test-Simple-1.001014.tar.gz
GEMPESAW/Selenium-Remote-Driver-0.24.tar.gz
HAOYAYOI/Net-APNS-0.0202.tar.gz
HIROSE/Devel-PatchPerl-Plugin-Legacy-0.03.tar.gz
HIROSE/Ganglia-Gmetric-XS-1.04.tar.gz
HIROSE/IPC-Lock-WithTTL-0.01.tar.gz
INGY/WikiText-Socialtext-0.20.tar.gz
KAZUHO/Parallel-Scoreboard-0.07.tar.gz
KITANO/Test-LoadAllModules-0.022.tar.gz
KITANO/Test-Perl-Metrics-Lite-0.2.tar.gz
KNEW/Finance-Bank-Natwest-0.05.tar.gz
KURIHARA/HTTP-MobileAgent-0.36.tar.gz
MALA/URI-CrawlableHash-0.02.tar.gz
MASAKI/HTTP-Router-0.05.tar.gz
MASAKI/MouseX-Param-0.01.tar.gz
MATTN/Win32-Console-GetC-0.01.tar.gz
MELO/AnyEvent-Gearman-0.10.tar.gz
MIKI/AnyEvent-Pcap-0.00002.tar.gz
MIKI/Lingua-JA-Categorize-0.02002.tar.gz
MIKI/Text-Bayon-0.00002.tar.gz
MINIMAL/Kelp-0.9051.tar.gz
MIYAGAWA/AnyEvent-FriendFeed-Realtime-0.05.tar.gz
MIYAGAWA/Catalyst-Engine-PSGI-0.13.tar.gz
MIYAGAWA/Encode-DoubleEncodedUTF8-0.05.tar.gz
MIYAGAWA/HTML-AutoPagerize-0.02.tar.gz
MIYAGAWA/Tatsumaki-0.1013.tar.gz
NCLEATON/Test-Group-0.19.tar.gz
NEKOKAK/Test-Declare-0.06.tar.gz
NINE/Test-WWW-WebKit-0.03.tar.gz
OISHI/DBIx-ObjectMapper-0.3013.tar.gz
OPI/Try-Tiny-Except-0.01.tar.gz
PETDANCE/Test-WWW-Mechanize-1.44.tar.gz
PETDANCE/ack-2.14.tar.gz
PORRIDGE/App-MaMGal-1.4.tar.gz
RHOELZ/Dist-Zilla-Plugin-Test-LocalBrew-0.08.tar.gz
RIBASUSHI/DBIx-Class-0.082820.tar.gz
RJBS/Test-Fatal-0.014.tar.gz
RJBS/perl-5.20.0.tar.gz
RKRIMEN/Test-Lazy-0.061.tar.gz
ROHANPM/Gerrit-Client-20140611.tar.gz
RSRCHBOY/Test-Moose-More-0.029.tar.gz
SARTAK/Jifty-1.10518.tar.gz
SARTAK/Jifty-Plugin-OAuth-0.04.tar.gz
SATOH/DBIx-RewriteDSN-0.05.tar.gz
SATOH/Encode-Base58-BigInt-0.03.tar.gz
SATOH/HTML-Trim-0.02.tar.gz
SATOH/List-Enumerator-0.10.tar.gz
SATOH/Plack-Middleware-StaticShared-0.05.tar.gz
SATOH/Test-HTML-Differences-0.03.tar.gz
SATOH/Test-Name-FromLine-0.13.tar.gz
SATOH/Test-Time-0.04.tar.gz
SATOH/Text-Xatena-0.18.tar.gz
SHLOMIF/XML-LibXML-2.0118.tar.gz
SPANG/App-SD-0.75.tar.gz
TANIGUCHI/Plack-Middleware-LimitRequest-0.02.tar.gz
THALJEF/Pinto-0.09997.tar.gz
TIMB/WebAPI-DBIC-0.004002.tar.gz
TNT/Syntax-Feature-Try-1.000.tar.gz
TOBYINK/Test-Modern-0.013.tar.gz
TOKUHIROM/Docopt-0.03.tar.gz
TOKUHIROM/Sledge-Utils-0.04.tar.gz
TOMITA/Acme-Ikamusume-0.07.tar.gz
TOMITA/Template-Semantic-0.09.tar.gz
TOSHIOITO/Async-Selector-1.03.tar.gz
TOSHIOITO/BusyBird-0.12.tar.gz
TSUCCHI/SQL-Executor-0.17.tar.gz
TYPESTER/AnyEvent-APNS-0.10.tar.gz
TYPESTER/AnyEvent-JSONRPC-Lite-0.15.tar.gz
TYPESTER/Array-Diff-0.07.tar.gz
VPIT/LaTeX-TikZ-0.02.tar.gz
XSAWYERX/Dancer2-0.159003.tar.gz
YAPPO/HTML-StickyQuery-DoCoMoGUID-0.03.tar.gz
YAPPO/HTTP-Engine-0.03005.tar.gz
YAPPO/Log-Dispatch-Screen-Color-0.04.tar.gz
ZIGOROU/DBIx-DBHResolver-0.17.tar.gz
ZIGOROU/XRI-Resolution-Lite-0.04.tar.gz
HERE

#--------------------------------------------------------------------------#
# modules that have Test::Builder->
#--------------------------------------------------------------------------#

# put in HERE doc from grep.cpan.me
my @test_builder_methods = split " ", <<HERE;
ADAMK/Test-Inline-2.213.tar.gz
AMS/Storable-2.51.tar.gz
ANDYA/TAP-Parser-0.54.tar.gz
BBC/Pinwheel-0.2.7.tar.gz
BDFOY/Test-Data-1.24.tar.gz
BINGOS/ExtUtils-Install-2.04.tar.gz
BINGOS/ExtUtils-MakeMaker-7.04.tar.gz
BOOK/HTTP-Proxy-0.302.tar.gz
BOWTIE/Test-Software-License-0.004000.tar.gz
CJFIELDS/BioPerl-DB-1.006900.tar.gz
CJFIELDS/BioPerl-Network-1.006902.tar.gz
CJFIELDS/BioPerl-Run-1.006900.tar.gz
CORION/Test-Exim4-Routing-0.02.tar.gz
DLY/App-Fetchware-1.014.tar.gz
DROLSKY/DateTime-1.18.tar.gz
EILARA/XUL-Node-0.06.tar.gz
ETHER/Dist-Zilla-Plugin-AuthorityFromModule-0.006.tar.gz
ETHER/Dist-Zilla-Plugin-CheckSelfDependency-0.011.tar.gz
ETHER/Dist-Zilla-Plugin-DynamicPrereqs-0.009.tar.gz
ETHER/Dist-Zilla-Plugin-Git-Contributors-0.011.tar.gz
ETHER/Dist-Zilla-Plugin-Keywords-0.006.tar.gz
ETHER/Dist-Zilla-Plugin-MakeMaker-Awesome-0.33.tar.gz
ETHER/Dist-Zilla-Plugin-OnlyCorePrereqs-0.024.tar.gz
ETHER/Dist-Zilla-Plugin-OptionalFeature-0.021.tar.gz
ETHER/Dist-Zilla-Plugin-Run-0.035.tar.gz
ETHER/Dist-Zilla-Plugin-Test-Compile-2.052.tar.gz
ETHER/Dist-Zilla-Plugin-Test-EOL-0.17.tar.gz
ETHER/Dist-Zilla-Plugin-Test-NoTabs-0.13.tar.gz
ETHER/Dist-Zilla-Plugin-TrialVersionComment-0.004.tar.gz
ETHER/Dist-Zilla-Plugin-VerifyPhases-0.010.tar.gz
ETHER/Dist-Zilla-Role-File-ChangeNotification-0.005.tar.gz
ETHER/Test-Class-0.48.tar.gz
ETHER/Test-NewVersion-0.003.tar.gz
ETHER/Test-Warnings-0.021.tar.gz
EXODIST/Fennec-2.017.tar.gz
EXODIST/Test-SharedFork-0.29.tar.gz
EXODIST/Test-Simple-1.001014.tar.gz
FRASE/Test-Builder-Clutch-0.07.tar.gz
GLENNWOOD/Scraper-3.05.tar.gz
GURUPERL/Net-XMPP3-1.02.tgz
HAOYAYOI/Net-APNS-0.0202.tar.gz
KENTNL/Generic-Assertions-0.001001.tar.gz
LEONT/Test-Harness-3.35.tar.gz
LUSHE/Egg-Release-JSON-0.02.tar.gz
LUSHE/Egg-Release-XML-FeedPP-0.02.tar.gz
LUSHE/HTML-Template-Associate-FormField-0.12.tar.gz
LUSHE/HTML-Template-Ex-0.08.tar.gz
MARKF/Test-Builder-Tester-1.01.tar.gz
MASAKI/HTTP-Router-0.05.tar.gz
MBARBON/Module-Info-0.35.tar.gz
MHX/Convert-Binary-C-0.76.tar.gz
MSCHWERN/Carp-Fix-1_25-1.000001.tar.gz
MSCHWERN/Test-Random-20130427.tar.gz
MSCHWERN/Test-Sims-20130412.tar.gz
MSCHWERN/mixin-0.07.tar.gz
NCLEATON/Test-Group-0.19.tar.gz
NCLEATON/Test-ParallelSubtest-0.05.tar.gz
OVID/Test-Class-Moose-0.58.tar.gz
OVID/Test-Most-0.34.tar.gz
PMQS/Archive-Zip-SimpleZip-0.009.tar.gz
PMQS/BerkeleyDB-0.55.tar.gz
PMQS/Compress-Raw-Bzip2-2.068.tar.gz
PMQS/Compress-Raw-Lzma-2.068.tar.gz
PMQS/Compress-Raw-Zlib-2.068.tar.gz
PMQS/IO-Compress-2.068.tar.gz
PMQS/IO-Compress-Lzf-2.068.tar.gz
PMQS/IO-Compress-Lzma-2.068.tar.gz
PMQS/IO-Compress-Lzop-2.068.tar.gz
POTYL/Gtk2-SourceView2-0.10.tar.gz
REATMON/Net-HTTPServer-1.1.1.tar.gz
REATMON/Net-Jabber-2.0.tar.gz
RIBASUSHI/DBIx-Class-0.082820.tar.gz
RJBS/perl-5.20.0.tar.gz
RWSTAUNER/Test-Aggregate-0.372.tar.gz
RYBSKEJ/forks-0.36.tar.gz
RYBSKEJ/forks-BerkeleyDB-0.06.tar.gz
SARTAK/Jifty-1.10518.tar.gz
SATOH/Encode-Base58-BigInt-0.03.tar.gz
SATOH/Plack-Middleware-StaticShared-0.05.tar.gz
SATOH/Test-Name-FromLine-0.13.tar.gz
SEMANTICO/Test-XML-0.08.tar.gz
SHLOMIF/Test-Run-0.0302.tar.gz
SMUELLER/PathTools-3.47.tar.gz
SPROUT/Convert-Number-Greek-0.02a.tar.gz
SPROUT/Sub-Delete-1.00002.tar.gz
SPROUT/Tie-Util-0.04.tar.gz
SPROUT/constant-lexical-2.0001.tar.gz
TOKUHIROM/Test-Kantan-0.40.tar.gz
TONYC/Imager-1.002.tar.gz
TSUCCHI/Test-Module-Used-0.2.6.tar.gz
VPIT/Test-Valgrind-1.14.tar.gz
XMATH/Data-Swap-0.08.tar.gz
XSAWYERX/Dancer2-0.159003.tar.gz
YANICK/Dancer-1.3134.tar.gz
YUPUG/Template-Plugin-Filter-HTMLScrubber-0.03.tar.gz
ZEFRAM/Data-Alias-1.18.tar.gz
ZIGOROU/Catalyst-View-Reproxy-0.05.tar.gz
HERE

#--------------------------------------------------------------------------#
# assemble and get sorted uniq set
#--------------------------------------------------------------------------#

my @consolidated =
  uniq sort grep { $_ !~ /^Test-Simple-\d/ } grep { $_ !~ /^perl-5/ }
  grep { exists $latest{$_} } @dependents, @test_modules, @test_builder_level,
  @test_builder_methods;

say for @consolidated;
