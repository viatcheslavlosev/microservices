/** функциональность класса SparkMainService представляет собой
 * микросервис, который возвращает среднее значение посещений сети
 * кафе за сутки в json формате при обращении по запросу
 * http://<host_name:8090>/getTrafficAvg
 * В качестве web-server использован Spark Java
 * В качестве inmemory БД использована H2 Database
 *
 * автор Viatcheslav Losev
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import static java.lang.Thread.sleep;
import static spark.Spark.*;

public class SparkMainService {


    private static Connection con;
    private static JSONObject cafe;
    private static JSONObject cafe1;
    private static JSONObject cafe2;
    private static JSONObject cafe3;
    private static JSONObject cafe4;
    private static JSONArray cafeList;

    public static void main(String[] args) {


        String url = "jdbc:h2:mem:";

        try {


            /** Получаем Connection H2
             */
             con = DriverManager.getConnection(url);
             Statement st = con.createStatement();
             Statement st1 = con.createStatement();

            /** Создаем и заполняем две сущности БД - кафе(идентификатор и имя) и посещения (кафе,
             количество, дата).
             */
             boolean rscr1 = st.execute("CREATE TABLE VISITS ( \n" +
                     "   cafe_id INT NOT NULL, \n" +
                     "   traffic_avg_day INT NOT NULL, \n" +
                     "   date_of_visits DATE NOT NULL,\n" +
                     ");");

            boolean rscr2 = st.execute("CREATE TABLE CAFE_NEW (  \n" +
                    "   ID INT NOT NULL, \n" +
                    "   name VARCHAR(50) NOT NULL, \n" +
                    ");");

            boolean rscrinscafen  = st.execute("INSERT INTO CAFE_NEW VALUES ( 1, 'Ramesh');");
            boolean rscrinscafen1 = st.execute("INSERT INTO CAFE_NEW VALUES ( 2, 'Victor');");
            boolean rscrinscafen2 = st.execute("INSERT INTO CAFE_NEW VALUES ( 3, 'Sochi');");
            boolean rscrinscafen3 = st.execute("INSERT INTO CAFE_NEW VALUES ( 4, 'Дорожное');");
            boolean rscrinscafen4 = st.execute("INSERT INTO CAFE_NEW VALUES ( 5, 'тетя Груша');");

            boolean rscrinsvisits  = st.execute("INSERT INTO VISITS VALUES (1, 1, '2019-05-21');");
            boolean rscrinsvisits1 = st.execute("INSERT INTO VISITS VALUES (2, 1, '2019-05-21');");
            boolean rscrinsvisits2 = st.execute("INSERT INTO VISITS VALUES (3, 1, '2019-05-21');");
            boolean rscrinsvisits3 = st.execute("INSERT INTO VISITS VALUES (4, 1, '2019-05-21');");
            boolean rscrinsvisits4 = st.execute("INSERT INTO VISITS VALUES (5, 1, '2019-05-21');");

            boolean rscrinsvisitsS  = st.execute("INSERT INTO VISITS VALUES (1, 1, '2019-05-22');");
            boolean rscrinsvisitsS1 = st.execute("INSERT INTO VISITS VALUES (2, 1, '2019-05-22');");
            boolean rscrinsvisitsS2 = st.execute("INSERT INTO VISITS VALUES (3, 1, '2019-05-22');");
            boolean rscrinsvisitsS3 = st.execute("INSERT INTO VISITS VALUES (4, 1, '2019-05-22');");
            boolean rscrinsvisitsS4 = st.execute("INSERT INTO VISITS VALUES (5, 1, '2019-05-22');");

            ResultSet rs = st.executeQuery("SELECT name FROM CAFE_NEW");
            ResultSet rs1 = st1.executeQuery("SELECT traffic_avg_day FROM VISITS");


            /** Формируем JSONArray cafeList с данными о кафе
             */
            cafe = new JSONObject();
            cafe1 = new JSONObject();
            cafe2 = new JSONObject();
            cafe3 = new JSONObject();
            cafe4 = new JSONObject();

            cafeList = new JSONArray();

            cafeList.add(cafe);
            cafeList.add(cafe1);
            cafeList.add(cafe2);
            cafeList.add(cafe3);
            cafeList.add(cafe4);


            String str;
            String str1;

             for(int i=0; i<cafeList.size(); i++ )  {
                rs.next();
                str = rs.getString(1);
                rs1.next();
                str1 = rs1.getString(1);
                JSONObject cafeListJson = (JSONObject) cafeList.get(i);

                cafeListJson.put("name", str);
                cafeListJson.put("traffic_avg_day", str1);

             }

            /** Запускаем отдельный поток для вычисления среднего количества посещений кафе
             */
              NumberVisits mThing = new NumberVisits();
              Thread myThready = new Thread(mThing);
              myThready.start();

            /** Запускаем Spark на port 8090, передаем JSONArray
             */
            port(8090);
            get("/getTrafficAvg", "application/json", (req, res) -> {  
                res.type("application/json");
                return cafeList;
            });


        } catch (SQLException ex) {

            Logger lgr = Logger.getLogger(SparkMainService.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }


    }


    private static class NumberVisits implements Runnable {

        public void run()
        {

            try {

                Statement stupd  = con.createStatement();
                Statement stupd1 = con.createStatement();
                Statement stupd2 = con.createStatement();
                Statement stupd3 = con.createStatement();
                Statement stupd4 = con.createStatement();
                int km;
                int km1;
                int km2;
                int km3;
                int km4;
                boolean iscis = true;
                int i = 0;
                String str_date = "";

            while(iscis) {

                /** Меняем день на другой (21.05.19 или 22.05.19) после генерации
                *   20-ти значений посещаемости
                */
                i++;
                if(i == 1)  {str_date = "2019-05-21"; }
                if(i == 21) {str_date = "2019-05-22"; }
                if(i == 40) {i = 0;}
                //System.out.println("str_date = " + str_date);

                /** Показатели посещений генерируются случайным
                 *  образом раз в 3 секунды для каждого кафе В ДИАПАЗОНЕ (1-10)
                 */
                try {
                    sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                    km  = new Random().nextInt(11);
                    km1 = new Random().nextInt(11);
                    km2 = new Random().nextInt(11);
                    km3 = new Random().nextInt(11);
                    km4 = new Random().nextInt(11);


                /** Обновляем показатели посещений в БД, выбираем обновленные данные.
                 */
                boolean rscrins   = stupd.execute("UPDATE VISITS SET VISITS.traffic_avg_day =" + km + "WHERE VISITS.cafe_id = (SELECT ID FROM CAFE_NEW WHERE name = 'Ramesh' and VISITS.date_of_visits = '" + str_date + "')");
                ResultSet rs = stupd.executeQuery("SELECT traffic_avg_day FROM VISITS INNER JOIN CAFE_NEW ON VISITS.cafe_id = CAFE_NEW.ID  WHERE CAFE_NEW.name = 'Ramesh' and VISITS.date_of_visits = '" + str_date + "'");

                boolean rscrins1   = stupd1.execute("UPDATE VISITS SET VISITS.traffic_avg_day =" + km1 + "WHERE VISITS.cafe_id = (SELECT ID FROM CAFE_NEW WHERE name = 'Victor' and VISITS.date_of_visits = '" + str_date + "')");
                ResultSet rs1 = stupd1.executeQuery("SELECT traffic_avg_day FROM VISITS INNER JOIN CAFE_NEW ON VISITS.cafe_id = CAFE_NEW.ID  WHERE CAFE_NEW.name = 'Victor' and VISITS.date_of_visits = '" + str_date + "'");

                boolean rscrins2   = stupd2.execute("UPDATE VISITS SET VISITS.traffic_avg_day =" + km2 + "WHERE VISITS.cafe_id = (SELECT ID FROM CAFE_NEW WHERE name = 'Sochi' and VISITS.date_of_visits = '" + str_date + "')");
                ResultSet rs2 = stupd2.executeQuery("SELECT traffic_avg_day FROM VISITS INNER JOIN CAFE_NEW ON VISITS.cafe_id = CAFE_NEW.ID  WHERE CAFE_NEW.name = 'Sochi' and VISITS.date_of_visits = '" + str_date + "'");

                boolean rscrins3   = stupd3.execute("UPDATE VISITS SET VISITS.traffic_avg_day =" + km3 + "WHERE VISITS.cafe_id = (SELECT ID FROM CAFE_NEW WHERE name = 'Дорожное' and VISITS.date_of_visits = '" + str_date + "')");
                ResultSet rs3 = stupd3.executeQuery("SELECT traffic_avg_day FROM VISITS INNER JOIN CAFE_NEW ON VISITS.cafe_id = CAFE_NEW.ID  WHERE CAFE_NEW.name = 'Дорожное' and VISITS.date_of_visits = '" + str_date + "'");

                boolean rscrins4   = stupd4.execute("UPDATE VISITS SET VISITS.traffic_avg_day =" + km4 + "WHERE VISITS.cafe_id = (SELECT ID FROM CAFE_NEW WHERE name = 'тетя Груша' and VISITS.date_of_visits = '" + str_date + "')");
                ResultSet rs4 = stupd4.executeQuery("SELECT traffic_avg_day FROM VISITS INNER JOIN CAFE_NEW ON VISITS.cafe_id = CAFE_NEW.ID  WHERE CAFE_NEW.name = 'тетя Груша' and VISITS.date_of_visits = '" + str_date + "'");

                /** Обновляем данные по посещениям в JSONArray cafeList
                 */
                String str;
                String str1;
                String str2;
                String str3;
                String str4;

                while (rs.next()) {
                    str = rs.getString(1);
                    cafe.replace("traffic_avg_day", str);
                }
                while (rs1.next()) {
                    str1 = rs1.getString(1);
                    cafe1.replace("traffic_avg_day", str1);
                }
                while (rs2.next()) {
                    str2 = rs2.getString(1);
                    cafe2.replace("traffic_avg_day", str2);
                }
                while (rs3.next()) {
                    str3 = rs3.getString(1);
                    cafe3.replace("traffic_avg_day", str3);
                }
                while (rs4.next()) {
                    str4 = rs4.getString(1);
                    cafe4.replace("traffic_avg_day", str4);
                }

                System.out.println("str_date = " + str_date);
                System.out.println(cafeList);
                System.out.println();

            }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}


