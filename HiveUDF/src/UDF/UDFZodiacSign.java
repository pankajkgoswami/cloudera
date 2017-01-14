package UDF;

import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
@Description(name = "zodiac",
value = "_FUNC_(date) - from the input date string "+
"or separate month and day arguments, returns the sign of the Zodiac.",
extended = "Example:\n"
+ " > SELECT _FUNC_(date_string) FROM src;\n"
+ " > SELECT _FUNC_(month, day) FROM src;")
public class UDFZodiacSign extends UDF{
private SimpleDateFormat df;
public UDFZodiacSign(){
df = new SimpleDateFormat("MM-dd-yyyy");
}

public String evaluate( Date bday ){
	Date date = null;
	Calendar cal = Calendar.getInstance();
	cal.setTime(date);
	return this.evaluate( cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH));
}

public String evaluate(String bday) throws ParseException{
Date date = null;
date = df.parse(bday);
Calendar cal = Calendar.getInstance();
cal.setTime(date);
try {
cal.get(Calendar.MONTH);
} catch (Exception ex) {
return null;
}
return this.evaluate( cal.get(Calendar.MONTH)+1, cal.get(Calendar.DAY_OF_MONTH));
}

public String evaluate( Integer month, Integer day ){
if (month==1) {
if (day < 20 ){
return "Capricorn";
} else {
return "Aquarius";
}
}
if (month==2){
if (day < 19 ){
return "Aquarius";
} else {
return "Pisces";
}
}
if (month==3){
if (day < 21 ){
return "Pisces";
} else {
return "Aries";
}
}
if (month==4){
if (day < 20 ){
return "Aries";
} else {
return "Taurus";
}
}

if (month==5){
if (day < 20 ){
return "Taurus";
} else {
return "Gemini";
}
}

if (month==6){
if (day < 21 ){
return "Gemini";
} else {
return "Cancer";
}
}
if (month==7){
if (day < 23 ){
return "Cancer";
} else {
return "Leo";
}
}
if (month==8){
if (day < 23 ){
return "Leo";
} else {
return "Virgo";
}
}
if (month==9){
if (day < 23 ){
return "Virgo";
} else {
return "Libra";
}
}
if (month==10){
if (day < 23 ){
return "Libra";
} else {
return "Scorpio";
}
}
if (month==11){
if (day < 22 ){
return "Scorpio";
} else {
return "Saggitarius";
}
}
if (month==2){
if (day < 19 ){
return "Saggitarius";
} else {
return "Capricorn";
}
}
return null;
}
}