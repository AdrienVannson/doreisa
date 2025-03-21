#include <mpi.h>
#include <pdi.h>
#include <cassert>
#include <iostream>
#include <cmath>

using namespace std;

// The global grid is a PROCESSOR_GRID_SIZE*PROCESSOR_GRID_SIZE grid, each cell being simulated by a processor
// PROCESSOR_GRID_SIZE = sqrt(NB_PROCESSORS)
const int PROCESSOR_GRID_SIZE = 2;

// Number of processors used in the simulation
const int NB_PROCESSORS = PROCESSOR_GRID_SIZE * PROCESSOR_GRID_SIZE;

// The current processor is responsible for the simulation of a LOCAL_SIZE * LOCAL_SIZE grid
const int LOCAL_SIZE = 3000;


const int TAG_CELL_VALUE = 0;

const int NB_SIMULATION_STEPS = 1000;

int RANK; // Rank of the current processor
int PROCESSOR_LINE, PROCESSOR_COLUMN;

// The values around the grid are received from the neighbors
double GRID[LOCAL_SIZE+2][LOCAL_SIZE+2];
double NEW_GRID[LOCAL_SIZE+2][LOCAL_SIZE+2]; // Used only during a simulation step


void syncGhostCells()
{
    // Synchronize the grid with the neighbors
    for (int it = 0; it < 2; it++) { // Vertical exchanges
        if (PROCESSOR_LINE != 0 && it == PROCESSOR_LINE % 2) {
            for (int i = 1; i <= LOCAL_SIZE; i++) {
                MPI_Send(&GRID[1][i], 1, MPI_DOUBLE, RANK - PROCESSOR_GRID_SIZE, TAG_CELL_VALUE, MPI_COMM_WORLD);
                MPI_Recv(&GRID[0][i], 1, MPI_DOUBLE, RANK - PROCESSOR_GRID_SIZE, TAG_CELL_VALUE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
        if (PROCESSOR_LINE != PROCESSOR_GRID_SIZE - 1 && it != PROCESSOR_LINE % 2) {
            for (int i = 1; i <= LOCAL_SIZE; i++) {
                MPI_Send(&GRID[LOCAL_SIZE][i], 1, MPI_DOUBLE, RANK + PROCESSOR_GRID_SIZE, TAG_CELL_VALUE, MPI_COMM_WORLD);
                MPI_Recv(&GRID[LOCAL_SIZE+1][i], 1, MPI_DOUBLE, RANK + PROCESSOR_GRID_SIZE, TAG_CELL_VALUE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
    }

    for (int it = 0; it < 2; it++) { // Horizontal exchanges
        if (PROCESSOR_COLUMN != 0 && it == PROCESSOR_COLUMN % 2) {
            for (int i = 1; i <= LOCAL_SIZE; i++) {
                MPI_Send(&GRID[i][1], 1, MPI_DOUBLE, RANK - 1, TAG_CELL_VALUE, MPI_COMM_WORLD);
                MPI_Recv(&GRID[i][0], 1, MPI_DOUBLE, RANK - 1, TAG_CELL_VALUE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
        if (PROCESSOR_COLUMN != PROCESSOR_GRID_SIZE - 1 && it != PROCESSOR_COLUMN % 2) {
            for (int i = 1; i <= LOCAL_SIZE; i++) {
                MPI_Send(&GRID[i][LOCAL_SIZE], 1, MPI_DOUBLE, RANK + 1, TAG_CELL_VALUE, MPI_COMM_WORLD);
                MPI_Recv(&GRID[i][LOCAL_SIZE+1], 1, MPI_DOUBLE, RANK + 1, TAG_CELL_VALUE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
    }
}

void simulationStep()
{
    const double ALPHA = 0.9;

    // Do the computations
    for (int i = 1; i <= LOCAL_SIZE; i++) {
        for (int j = 1; j <= LOCAL_SIZE; j++) {
            NEW_GRID[i][j] = GRID[i][j] + ALPHA * (GRID[i+1][j] + GRID[i-1][j] + GRID[i][j+1] + GRID[i][j-1] - 4 * GRID[i][j]) / 4;
        }
    }

    // Save the results
    for (int i = 1; i <= LOCAL_SIZE; i++) {
        for (int j = 1; j <= LOCAL_SIZE; j++) {
            GRID[i][j] = NEW_GRID[i][j];
        }
    }
}

int main(int argc, char* argv[])
{
    MPI_Init(NULL, NULL);

    int nbProcessors;
    MPI_Comm_size(MPI_COMM_WORLD, &nbProcessors);
    assert(nbProcessors == NB_PROCESSORS);

    MPI_Comm_rank(MPI_COMM_WORLD, &RANK);
    PROCESSOR_LINE = RANK / PROCESSOR_GRID_SIZE;
    PROCESSOR_COLUMN = RANK % PROCESSOR_GRID_SIZE;

    PDI_init(PC_parse_path("pdi_config.yml"));
    PDI_multi_expose("init", "rank", &RANK, PDI_OUT, NULL);

    // Initialize the grid
    for (int i = 1; i <= LOCAL_SIZE; i++) {
        for (int j = 1; j <= LOCAL_SIZE; j++) {
            float x = (PROCESSOR_LINE + i / (float) LOCAL_SIZE) / PROCESSOR_GRID_SIZE;
            float y = (PROCESSOR_COLUMN + j / (float) LOCAL_SIZE) / PROCESSOR_GRID_SIZE;

            GRID[i][j] = sin(2 * x * 2 * M_PI) * cos(2 * y * 2 * M_PI);
        }
    }

    // Run the simulation
    for (int t = 0; t < NB_SIMULATION_STEPS; t++) {
        syncGhostCells();
        simulationStep();

        PDI_multi_expose("simulation_step", "temperatures", GRID, PDI_OUT, NULL);
    }

    PDI_finalize();

    MPI_Finalize();
    return 0;
}
