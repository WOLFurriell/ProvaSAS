/*EXERCÍCIO 1*/
/*A) e B)*/
proc delete data=_all_;
run;

/*IMPORTAR O BANCO*/
filename dados "D:\Estatística\ESTATÍSTICA COMPUTACIONAL II\Bancos\Prova\CE82686_2.txt";
data meter12(keep= estacao data hora precipitacao tempminima where=(hora=1200)) 
     meter0(drop=precipitacao tempminima where=(hora=0));
	informat data ddmmyy10.;
	infile dados dlm = ";" dsd missover;
	input Estacao$ Data Hora Precipitacao TempMaxima TempMinima Insolacao Evaporacao_Piche Tem_Comp_Media Umidade_Relativa_Media Velocidade_do_vento_Media;
run;

proc sort data=meter12 out=meter12;
by data;
run;

proc sort data=meter0 out=meter0;
by data;
run;

/*REALIZAR MERGE ENTRE O BANCO DAS 12H E O DAS 0H*/
data meter_all;
merge meter0 (in=x) meter12 (in=y);
by data;
	if (x=1 & y=1);
		dia=day(data);
		mes=month(data);
		ano = year(data);
		aux=intnx('month', data, 0, 'end'); /*na variavel data de 0 até o fim do mês*/
		numday=day(aux); /* Verificar o número de dias que o mês tem*/
run;

/*CRIAR UM CONTADOR PARA OS DIAS EM QUE OCORREU MEDIÇÃO DENTRO DE UM MÊS*/
data teste(drop=hora -- TempMinima);
	set meter_all;
	real + 0; 
	by ano mes dia;
	if (first.mes)then real = real+1;
run;

/*VERIFICAR O NUMERO DE DIAS EM QUE OCORREU MEDIÇÃO DA PRECIPITACAO DENTRO DE UM MÊS*/
data  verif_days;
	set teste;
	by real;
		if (first.real)then cont=0;
		cont+1;
			if (last.real)then output;
run;

/*ATRIBUIR OS DIAS DE MEDICAO POR MÊS A CADA LINHA DO SAS DATASET*/
proc sql noprint;
	create table meter3 as select
	*,
	count(precipitacao) as casos 
		from meter_all 
			group by ano, mes; 
quit;

/*SELECIONAR APENAS OS MESES EM QUE OCORRERAM MEDIÇÕES TODOS OS DIAS*/
data meter1;
	set meter3;
	if (casos=numday) then delete;
run;

data meter(keep= precipitacao data dia mes ano numday casos);
	format data ddmmyy.;
	set meter;
run;

proc sort data=meter;
	by precipitacao;
run;

/*CRIAR UM AUXILIXAR PARA OS DIAS ONDE PRECIPITAÇAO FOI MENOS QUE 5*/
data medir_5;
  set meter;
  id_1 + 0; 
	  by precipitacao;
		  if precipitacao<5 then id_1 = 1;
		  else id_1=0;
run;

proc sort data=medir_5;
	by data;
run;

/*VERIFICAR A SEQUÊNCIA DE DIAS EM QUE A PRECIPITACAO FOI MENOR QUE 5*/
data medir_5;
	set medir_5;
	Nrain + 0; 
	by data id_1;
		if (first.id_1 and id_1=1)then Nrain = Nrain+1;
		else Nrain = .; 
run;

/*ORDERNAR POR Nrain DIAS EM QUE NÃO CHOVEU*/
proc sort data=medir_5;
	by ano mes descending Nrain;
run;

/*PEDAR O MÁXIMO DE DIAS EM QUE NÃO CHOVEU POR MES E ANO*/
proc sql;
	create table dry as select 
	mes,
	ano, 
	max(Nrain) as max_seq_dry
		from medir_5
		 where (id_1=1) 
			group by mes, ano;
quit; 

proc sort data=dry;
	by descending max_seq_dry;
run;

/*MOSTRAR OS 10 MAIORES*/
proc print data=dry(obs=10);
run;

/*######################################################################################################################*/
/*C)*/
proc sql;
	create table meter_all2 as select
	*
		from meter_all 
			where precipitacao > 0;
run;

data meterQ;
	set meter_all2;
	leve=q1(precipitacao);
run; 

/*CRIAR OS PONTOS DE CORTE*/
proc means data=meter_all2;
	var precipitacao; 
	output out=cut q1=qua1 mean=media q3=qua3;
run;

/*CRIAR UM 'MACRO' COM OS NOMES E INFORMAÇÕES DOS CORTES*/
data _null_;
	set cut;
		call symput('q1',qua1) ;
		call symput('mean',media) ;
		call symput('q3',qua3) ;
run;

/*APLICANDO OS CORTES PARA A RECODIFICAÇÃO*/
data meter_class;
	set meter_all2;
	if (precipitacao<&q1) then classe=1;
	if (precipitacao>=&q1) and (precipitacao<&mean) then classe=2;
    if (precipitacao>=&mean) and (precipitacao<&q3) then classe=3;
    if (precipitacao>=&q3) then classe=4;
run;

/*CONFERINDO SE A GAMBIARRA DEU CERTO*/
data meter_class;
	set meter_class;
	rec_prec=.;
	if (precipitacao<1.1) then rec_prec=1;
	if (precipitacao>=1.1) and (precipitacao<11.2034017) then rec_prec=2;
    if (precipitacao>=11.2034017) and (precipitacao<14.0) then rec_prec=3;
    if (precipitacao>=14.0) then rec_prec=4;
run;

/*COLOCANDO OS NOMES NA RECODIFIÇÃO*/
proc format;
value  rec_prec 1="leve"
                2="média" 
				3="moderada"
				4="intensa";
value  classe   1="leve"
                2="média" 
				3="moderada"
				4="intensa";
run;              

proc freq data=meter_class;
format rec_prec rec_prec.;
format classe classe.;
tables rec_prec classe;
run;

/*#########################################*/
/*2. B) */
/*#########################################*/

proc delete data=_all_;
run;

proc import datafile="D:/Estatística/ESTATÍSTICA COMPUTACIONAL II/Bancos/Prova/x.txt" out=matrizx
dbms=csv replace;
run;

data MinMean_by_Rows;
   set matrizx;
   array x _numeric_;   
   media = mean(of x[*]);   /* Minimo por linha */
   minimo = min(of x[*]);    /* Média por linha */
run;

/*REALIZANDO A SUBSTITUIÇÃO DO MINIMO PELA MÉDIA DA LINHA*/
data x_recod; 
set matrizx; 
media=mean(of V1 - V10);
array ind[*] V1 - V10; /* criando um array que contém os valores de V1 a V10. */ 
	minimo=min(of ind[*]); /* usando minimo para identificar o menor valor da linha.*/ 
	do i=1 to dim(ind);  
		if ind[i] = minimo then do; /* Loop através do array procurando os casos que cumprem a condição citada*/
		ind[i] = mean(of ind[*]); /* Quando encontrado o minino substituir pela média */ 
		output;
	end; 
end; 
run; 

proc import datafile="D:/Estatística/ESTATÍSTICA COMPUTACIONAL II/Bancos/Prova/x2.txt" out=matrizNA
dbms=csv replace;
run;

/*SUBSTITUINDO OS NA*/
data y_recod; 
set matrizna; 
miss=nmiss(of V1 - V10);
array ind[*] V1-V10;
	do i=1 to dim(ind);  
	if ind[i] = 'NA' then do;
	ind[i] = mean(of ind[*]);
    end; 
end; 
run; 

