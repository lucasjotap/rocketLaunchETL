from ABC import abstractmethod

class Job:
    """
    Abstract base class for defining jobs.

    Attributes:
        job_name (str): The name of the job.
    """

    @property
    @abstractmethod
    def job_name(self) -> str:
        """
        Property representing the name of the job.

        Returns:
            str: The name of the job.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self) -> None:
        """
        Abstract method to execute the job.

        This method should contain the main logic of the job.

        Returns:
            None
        """
        raise NotImplementedError
