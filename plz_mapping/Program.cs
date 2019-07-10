using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;
using System.Threading;

namespace plz_Mapping
{
    class Program
    {
        private static string database;
        static SQLiteConnection sqlCon;
        static SQLiteCommand plzcommand;
        static SQLiteDataReader plzreader;
        static SQLiteCommand ortcommand;
        static SQLiteDataReader ortreader;
        static SQLiteCommand insertCommPLZ;
        static SQLiteCommand insertCommORT;
        static SQLiteCommand mappingComm;
        static SQLiteDataReader mappingreader;
        static SQLiteCommand mappingInsert;
        static string PLZID;
        static string ORTID;

        static void Main(string[] args)
        {



            string[] rohdaten = File.ReadAllLines(@"Z:\Paddy\Bitte aufräumen\plzDeutschland.csv", System.Text.Encoding.UTF8);
            long i = 1;
            long citynotFound = 1;
            long zipNotfound = 1;
            //database = @"D:\SourceCode\VisualStudio\Dogginator_Product\Dogginator\DoggiNator.db";
            sqlCon = new SQLiteConnection(@"Data Source =D:\SourceCode\VisualStudio\Dogginator_Product\Dogginator\DoggiNator.db; Version=3;New=True;");
            sqlCon.Open();

            foreach (string s in rohdaten)
            {

                if (s.Length > 0 && s.Contains(';'))
                {
                    string[] splitted = s.Split(';');
                    string plznew = splitted[0];
                    string ortnew = splitted[1];
                    string idPLZ;
                    string idORT;

                    //hier select id auf db mit where plz = plznew

                    if (plznew.Length == 4)
                    {
                        plznew = "0" + plznew;
                    }
                    plzcommand = new SQLiteCommand(sqlCon);
                    plzcommand.CommandText = "SELECT id FROM zipcode WHERE zip=\"" + plznew + "\";";
                    ortcommand = new SQLiteCommand(sqlCon);
                    ortcommand.CommandText = "SELECT id FROM city WHERE name=\"" + ortnew + "\";";
                    plzreader = plzcommand.ExecuteReader();
                    ortreader = ortcommand.ExecuteReader();
                    if (plzreader.HasRows)
                    {
                        while (plzreader.Read())
                        {
                            //    Console.WriteLine(plzreader["id"] );
                            idPLZ = plzreader[0].ToString();
                            PLZID = idPLZ;
                        }
                    }
                    else
                    {
                        insertCommPLZ = new SQLiteCommand(sqlCon);
                        insertCommPLZ.CommandText = "INSERT INTO zipcode (id,zip) VALUES(NULL" + "," + "'" + plznew + "'" + ");";
                        insertCommPLZ.ExecuteNonQuery();
                        idPLZ = zipNotfound.ToString();
                        PLZID = idPLZ;
                        zipNotfound++;
                    }
                    if (ortreader.HasRows)
                    {
                        while (ortreader.Read())
                        {
                            idORT = ortreader[0].ToString();
                            ORTID = idORT;
                        }

                    }
                    else
                    {

                        insertCommORT = new SQLiteCommand(sqlCon);
                        insertCommORT.CommandText = "INSERT INTO city (id,name) VALUES(NULL" + "," + "'" + ortnew + "'" + ");";
                        insertCommORT.ExecuteNonQuery();
                        idORT = citynotFound.ToString();
                        ORTID = idORT;
                        citynotFound++;

                    }
                    //wenn was zurück kommt dann war schon da. und die id ist uns schon bekann (aus dem gerade eben select)
                    //genauso select id auf ort where ort=  ortnew
                    //wenn was zurück kommt dann war schon da.

                    mappingComm = new SQLiteCommand(sqlCon);
                    mappingComm.CommandText = "SELECT id FROM ziptocity WHERE id_zip=\"" + PLZID + "\" AND id_city=\"" + ORTID + "\";";
                    mappingreader = mappingComm.ExecuteReader();

                    if (mappingreader.HasRows == false)
                    {

                        mappingInsert = new SQLiteCommand(sqlCon);
                        mappingInsert.CommandText = "INSERT INTO ziptocity (id,id_city,id_zip) VALUES(NULL" + "," + "'" + ORTID + "'" + "," + "'" + PLZID + "'" + ");";
                        mappingInsert.ExecuteNonQuery();
                        Console.WriteLine("Datensatz " + i + " von Datensatz: " + rohdaten.Length + " in Datenbank geschrieben");
                    }
                }
                i++;
                Thread.Sleep(25);
            }
        }
    }
}
