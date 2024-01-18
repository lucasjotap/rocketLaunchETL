from ABC import abstractmethod

	
class Job:
	@abstractmethod
	@property
	def job_name(self) -> str:
		raise NotImplementedError

	@abstractmethod
	def run(self) -> None:
		raise NotImplementedError
