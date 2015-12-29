use strict;

#########################################################
#

## Constantes que definem o volume de useCase10 a ser gerado
#my $NUM_NF=10000000;
my $NUM_NF=10000;
my $MAX_ITEM_NF=10;
my $NUM_CLIENTES=10000;
my $NUM_REVENDAS=10000;
my $NUM_VEND=@ARGV[0];

sub lpad
{
	my $str = $_[0];
	my $len = $_[1];
	my $chr = $_[2];
	
	$chr = " " unless (defined($chr));

	return substr(($chr x $len) . $str, -1 * $len, $len);
}

if (! -d useCase10)
{
	mkdir "useCase10";
}

#cliente
open F_CLIENTE, "> useCase10/dm_cliente.txt";

for (my $i = 1; $i <= $NUM_CLIENTES; $i++)
{
	print F_CLIENTE "$i;Cliente $i;\n";
}

close F_CLIENTE;

#tipo produto
open F_TIPO_PROD, "> useCase10/dm_tipo_prod.txt";

print F_TIPO_PROD "1;Cerveja;\n";
print F_TIPO_PROD "2;Refrigerante;\n";
print F_TIPO_PROD "3;Chopp;\n";

close F_TIPO_PROD;

#produto
open F_PROD, "> useCase10/dm_prod.txt";

my %prod;

$prod{1}=1;
$prod{2}=1;
$prod{3}=1;
$prod{4}=1;
$prod{5}=1;
$prod{6}=1;

print F_PROD "1;1;Brahma;\n";
print F_PROD "1;2;Skol;\n";
print F_PROD "1;3;Stella Artois;\n";
print F_PROD "1;4;Budweiser;\n";
print F_PROD "1;5;Leffe;\n";
print F_PROD "1;6;Hoegaarden;\n";

$prod{7}=2;
$prod{8}=2;
$prod{9}=2;

print F_PROD "2;7;Guarana;\n";
print F_PROD "2;8;Soda;\n";
print F_PROD "2;9;Pepsi;\n";

$prod{10}=3;
$prod{11}=3;

print F_PROD "3;10;Brahma Claro;\n";
print F_PROD "3;11;Brahma Escuro;\n";

close F_PROD;

#revenda
open F_REVENDA, "> useCase10/dm_revenda.txt";

for (my $i = 1; $i <= $NUM_REVENDAS; $i++)
{
	print F_REVENDA "$i;Revenda $i;\n";
}

close F_REVENDA;

#vendedor
open F_VEND, "> useCase10/dm_vendedor.txt";

my %vend;

for (my $i = 1; $i <= $NUM_VEND; $i++)
{
	my $rev = int(rand(10000)) + 1;
	$vend{$i} = $rev;
	print F_VEND "$rev;$i;Vendedor $i;\n";
}

close F_VEND;

#nf e item nf
open F_NF, "> useCase10/dm_nf.txt";
open F_I_NF, "> useCase10/dm_item_nf.txt";

for (my $nf = 1; $nf <= $NUM_NF; $nf++)
{
	my $num_itens = int(rand($MAX_ITEM_NF)) + 1;
	my $cli = int(rand($NUM_CLIENTES)) + 1;
	my $ven = int(rand($NUM_VEND)) + 1;
	my $rev = $vend{$ven};
	my $val_nf = (int(rand(500)) + 1) * $num_itens;
	my $val_imp = int($val_nf * 0.05);
	my $dia = lpad(int(rand(28)) + 1,2,"0");
	my $mes = lpad(int(rand(12)) + 1,2,"0");
	my $ano = 2013;
	my $data = "${ano}-${mes}-${dia}";
	
	print F_NF "$nf;$cli;$rev;$ven;$data;$val_nf;$val_imp;\n";
	
	for (my $i_nf = 1; $i_nf < $num_itens; $i_nf++)
	{
		my $prod = int(rand(11)) + 1;
		my $tipo_prod = $prod{$prod};
		my $val_item  = $val_nf / $num_itens;
		print F_I_NF "$nf;$cli;$rev;$ven;$data;$tipo_prod;$prod;$i_nf;$val_item;\n";
	}
}

close F_NF;
close F_I_NF;
