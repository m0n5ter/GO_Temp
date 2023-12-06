namespace GO.Workerservice.Model;

public enum ScaleState 
{
    NotOk = -1,
    Ok = 0,
    Unstable = 1,
    MultipleItems = 3,
    Underloaded = 4,
    Overloaded = 5,
}