"""
In this module we benchmark python's multithreading and multiprocessing. We are doing I/O operations  
and Compute Intesive Operations to see which method of concurrency or parallism performs better in 
which scenario. The concurrent.futures module is used to perform the multithreading and multiprocessing
functionality.

"""

from time import time, sleep
import requests
import concurrent.futures
import uuid
from os import listdir
from PIL import Image

# URLs for images to be downloaded
img_urls = ['https://images.unsplash.com/photo-1583800949141-7937c40b9d5d?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1587889087972-8b1709658695?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1588424357729-cbb26ba0b4a1?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590927490888-6a3033672ed6?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590861115101-ef19a4191cd2?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590879933004-de3284f7b568?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932477012-18d79ecc49f9?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590933937035-8a095cd97175?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932556943-43bed3cc1cca?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590893464443-5872216c1398?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1583800949141-7937c40b9d5d?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1587889087972-8b1709658695?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1588424357729-cbb26ba0b4a1?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590927490888-6a3033672ed6?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590861115101-ef19a4191cd2?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590879933004-de3284f7b568?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932477012-18d79ecc49f9?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590933937035-8a095cd97175?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932556943-43bed3cc1cca?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590893464443-5872216c1398?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1583800949141-7937c40b9d5d?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1587889087972-8b1709658695?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1588424357729-cbb26ba0b4a1?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590927490888-6a3033672ed6?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590861115101-ef19a4191cd2?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590879933004-de3284f7b568?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932477012-18d79ecc49f9?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590933937035-8a095cd97175?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932556943-43bed3cc1cca?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590893464443-5872216c1398?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1583800949141-7937c40b9d5d?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1587889087972-8b1709658695?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1588424357729-cbb26ba0b4a1?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590927490888-6a3033672ed6?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590861115101-ef19a4191cd2?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590879933004-de3284f7b568?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932477012-18d79ecc49f9?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590933937035-8a095cd97175?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932556943-43bed3cc1cca?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590893464443-5872216c1398?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1583800949141-7937c40b9d5d?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1587889087972-8b1709658695?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1588424357729-cbb26ba0b4a1?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590927490888-6a3033672ed6?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=60',
            'https://images.unsplash.com/photo-1590861115101-ef19a4191cd2?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590879933004-de3284f7b568?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932477012-18d79ecc49f9?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590933937035-8a095cd97175?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590932556943-43bed3cc1cca?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            'https://images.unsplash.com/photo-1590893464443-5872216c1398?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=700&q=60',
            ]


# Helper Methods to download images, process images and for cpu computations.
def download_img(img_url):
    """
    This mehtod takes the img url to be downloaded as argument and downloads the given image and saves it
    with a unique identifier from uuid the imgs folder.
    :param img_url: the img url to be downloaded
    :return: None
    """

    img = requests.get(img_url).content
    imgname = 'imgs/img-'+str(uuid.uuid4())+'.jpg'
    with open(imgname, 'wb') as f:
        f.write(img)
        print('img downloaded...')


# A decorator that calculates the execution time of given method
def calculate_exe_time(input_function):
    """
    This decorator method take in a function as argument and calulates its execution time.
    :param input_function: name of method to be executed.
    :return process_time: method that calls the input_function and calculates execution time.
    """

    def process_time(*args):
        start = time()
        input_function(*args)
        end = time()
        print(f"Execution time: {end-start} secs")
    return process_time


def process_img(filename):
    """
    This method take the filename of the img file, performs a gray scale operation on the file, creates
    a thumbnail of the img and then saves the file in processed folder.
    :param filename: name of img file.
    :return: None
    """

    img = Image.open(f'.\\imgs\\{filename}')
    gray_img = img.convert('L')
    gray_img.thumbnail((200, 400))
    gray_img.save(f'.\\processed\\{filename}')


def cpu_intensive_process(iteration):
    """
    This mehtod performs the the cpu extensive operation. It increments the count for each number in range
    of 9^9.
    :param iteration: 
    :return: None
    """

    # print(iteration)
    count = 0
    for i in range(9**9):
        count += i


# Mehtods to download images via multi threading or multi processing
def download_img_via_threading(img_urls):
    """
    Takes in img urls list as input and downloads imgs using multithreading in concurrent.futures
    :param img_urls: list of img urls to be downloaded
    :return: None
    """

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(download_img, img_urls)


def download_img_via_multiprocessing(img_urls):
    """
    Takes in img urls list as input and downloads imgs using multiprocessing in concurrent.futures
    :param img_urls: list of img urls to be downloaded
    :return: None
    """

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(download_img, img_urls)


# Mehtods that perform extensive I/O operations.
@calculate_exe_time
def io_operations_without_concurrency(file_names):
    """
    This method calls the process_img method for each file which performs simple image processing 
    on the img file and then moves the files to processed folder. This is done in a regular fashion 
    without any concurrency.
    :param file_names: list containing names of the img files to process
    :return: None
    """

    for filename in file_names:
        process_img(filename)


@calculate_exe_time
def io_operations_via_threading(file_names):
    """
    This method calls the process_img method for each file which performs simple image processing 
    on the img file and then moves the files to processed folder. This is done in a concurrent fashion
    using the multithreading feature in concurrent.futures module.
    :param file_names: list containing names of the img files to process
    :return: None
    """

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_img, file_names)


@calculate_exe_time
def io_operations_via_multiprocessing(file_names):
    """
    This method calls the process_img method for each file which performs simple image processing 
    on the img file and then moves the files to processed folder. This is done in a concurrent fashion using the multiprocessing feature in 
    concurrent.futures module.
    :param file_names: list containing names of the img files to process
    :return: None
    """

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(process_img, file_names)


# Mehtods that perform extensive CPU computations.
@calculate_exe_time
def cpu_computation_without_concurrency():
    """
    This method calls the cpu_intensive_process method which performs a CPU extensive operation.
    This is done in a regular fashion without any concurrency.
    :return: None
    """

    for i in range(4):
        cpu_intensive_process(i)


@calculate_exe_time
def cpu_computation_via_threading():
    """
    This method calls the cpu_intensive_process method which performs a CPU extensive operation.
    This is done in a concurrent fashion using the multithreading feature in concurrent.futures module.
    :return: None
    """

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(cpu_intensive_process, range(4))


@calculate_exe_time
def cpu_computation_via_multiprocessing():
    """
    This method calls the cpu_intensive_process method which performs a CPU extensive operation.
    This is done in a concurrent fashion using the multiprocessing feature in concurrent.futures module.
    :return: None
    """

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(cpu_intensive_process, range(4))


if __name__ == "__main__":

    # Uncomment the method you want to run.

    ##################################################
    ###    Downloading Images with concurrency     ###
    ##################################################

    # with concurrent.futures threading
    # download_img_via_threading(img_urls)

    # with concurrent.futures processing
    # download_img_via_multiprocessing(img_urls)

    # get list of all img file names in imgs folder
    file_names = listdir('.\\imgs')

    #######################################
    ###     I/O Extentisve Tasks        ###
    #######################################

    # I/O operations without concurrency
    # io_operations_without_concurrency(file_names)

    # I/O operations with multi-threading
    # io_operations_via_threading(file_names)

    # I/O operations with multi-processing
    # io_operations_via_multiprocessing(file_names)

    #######################################
    ###    Compute Extentisve Tasks     ###
    #######################################

    # CPU computations without concurrency
    # cpu_computation_without_concurrency()

    # CPU computations without concurrency
    # cpu_computation_via_threading()

    # CPU computations without concurrency
    cpu_computation_via_multiprocessing()
