open DP, '>', 'dailyForecast.csv' or die;
open HP, '>', 'hourlyForecast.csv' or die;
open HA, '>', 'hourlyActual.csv' or die;
open DA, '>', 'dailyActual.csv' or die;
#print DP "zip,collected_time,collected_date,forecast_date,high,precip_chance\n";
#print HP "zip,collected_time,collected_date,forecast_date,hour,temp,precip_chance\n";
#print HA "zip,collected_time,collected_date,forecast_date,hour,temp,conditions\n";
#print DA "zip,collected_time,collected_date,forecast_date,high,precip_amount\n";
foreach my $f (glob "data/*.dat") {
	next unless $f=~/data.(\d+).dat/;
	my $zip = $1;
	open F, $f or die "failed";
	my (@hp, @dp, @da, @ha, %hphash, %dphash, %dahash, %hahash);
	%hphash = ();
	%dphash = ();
	%dahash = ();
	%hahash = ();
	while (<F>) {
		chomp;
		if (/#finished/) {
			/T(\d\d)(:\d\d:\d\d)/;
			my $h = $1 -1;
			$h = "0$h" if $h < 10;
			my $t = $h.$2;
			foreach my $v (@hp) {
				print HP join (',', $zip, $t, @$v), "\n";
			}
			foreach my $v (@dp) {
				print DP join (',', $zip, $t, @$v), "\n";
			}
			foreach my $v (@da) {
				print DA join (',', $zip, $t, @$v), "\n";
			}
			foreach my $v (@ha) {
				print HA join (',', $zip, $t, @$v), "\n";
			}
			(@hp, @dp, @da, @ha) = ();
		} else {
			my ($k, @c) = split /\s*;\s*/;
			my $key = join ',', $c[0], $c[1];
			if ($k eq 'dailyPred') {
				push @c, "" while @c < 4;
				next if $dphash{$key};
				$dphash{$key} = 1;
				$c[3] = "" if $c[3]>100;
				push @dp, [@c];
			} elsif ($k eq 'hourlyPred') {
				my $last = 0;
				next if $hphash{$key};
				$hphash{$key} = 1;
				for (my $i = 2; $i < @c; $i+=3) {
					next if $c[$i] > 12;
					next if $c[$i] == "";
					$c[$i] = 0 if $last == 0 and $c[$i] == 12;
					$c[$i] += 12 if $last > $c[$i];
					$c[$i+2] = "" if $c[$i+1] == $c[$i+2];
					next if $c[$i] > 24;
					push @hp, [$c[0], $c[1], $c[$i], $c[$i+1], $c[$i+2]];
					$last = $c[$i];
				}
			} elsif ($k eq 'dailyActual') {
				push @c, "" while @c < 4;
				next if $dahash{$key};
				$dahash{$key} = 1;
				$c[3] = 0 if $c[3]>10;
				push @da, [@c];
			} elsif ($k eq 'hourlyActual') {
				my $last = 0;
				next if $hahash{$key};
				$hahash{$key} = 1;
				for (my $i = 2; $i < @c; $i+=3) {
					next if $c[$i] > 12;
					next if $c[$i] == "";
					$c[$i] = 0 if $last == 0 and $c[$i] == 12;
					$c[$i] += 12 if $last > $c[$i];
					next if $c[$i] > 24;
					push @ha, [$c[0], $c[1], $c[$i], $c[$i+1], $c[$i+2], "", ""];
					$last = $c[$i];
				}
			}
		}
	}
}