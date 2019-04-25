//Problem Scenario 34 : You have given a file named spark6/user.csv.

val textoLeido=sc.textFile("/cloudera/ejercicios_spark/escenario34");
val head=textoLeido.first;
val sincabecera=textoLeido.filter(x=>x!=head);
val RDDFinal=sincabecera.map((x)=>{var aux=x.split(','); (aux(0),aux(1),aux(2))})
val RDDFiltrado=RDDFinal.filter(x=>x._1!="myself")