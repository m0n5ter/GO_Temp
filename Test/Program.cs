using System.Data.Odbc;

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

OdbcConnection connection = new() 
{
    ConnectionString = @"Driver={SQL Anywhere 10};DatabaseName=godus;EngineName=test;uid=budde;pwd=k1O922\ED03W;LINKs=tcpip(host=192.168.103.201)"
};

connection.Open();
var command = connection.CreateCommand();
command.CommandText = "SELECT count(*) from DBA.TB_AUFTRAG";
var count = command.ExecuteScalar();
connection.Close();